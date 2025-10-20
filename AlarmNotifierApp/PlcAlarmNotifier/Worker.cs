using MailKit.Net.Smtp;
using MimeKit;
using S7.Net;
using System.Collections.Concurrent;

namespace PlcAlarmNotifier;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _config;

    // Per-tag state: last bit, debounce counter, last email time, and an "armed" flag (re-arm on falling edge)
    private readonly ConcurrentDictionary<string, (bool last, int stable, DateTime lastSent, bool armed)> _state = new();

    public Worker(ILogger<Worker> logger, IConfiguration config)
    {
        _logger = logger;
        _config  = config;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _logger.LogInformation("PLC Alarm Notifier starting...");

        var plcs            = _config.GetSection("Plcs").Get<List<PlcConfig>>() ?? new();
        var emailCfg        = _config.GetSection("Email").Get<EmailConfig>()     ?? throw new InvalidOperationException("Missing Email config");
        var recipientGroups = _config.GetSection("RecipientGroups").Get<Dictionary<string, string[]>>() ?? new();

        int pollMs         = _config.GetValue("PollingMs", 750);
        int debounceCycles = _config.GetValue("DebounceCycles", 2);
        var rateLimit      = TimeSpan.FromMinutes(_config.GetValue("RateLimitMinutes", 0)); // 0 = disabled

        // Start one poller per PLC (parallel)
        var tasks = plcs.Select(p => PollPlcLoop(p, recipientGroups, emailCfg, pollMs, debounceCycles, rateLimit, ct));
        await Task.WhenAll(tasks);
    }

    private async Task PollPlcLoop(
        PlcConfig plcCfg,
        Dictionary<string, string[]> groups,
        EmailConfig emailCfg,
        int pollMs,
        int debounceCycles,
        TimeSpan rateLimit,
        CancellationToken ct)
    {
        var cpu = Enum.Parse<CpuType>(plcCfg.CpuType, ignoreCase: true);
        using var plc = new Plc(cpu, plcCfg.Ip, plcCfg.Rack, plcCfg.Slot);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (!plc.IsConnected) plc.Open();

                foreach (var tag in plcCfg.Tags)
                {
                    bool bit = ReadBit(plc, tag);
                    string key = $"{plcCfg.Name}.{tag.Name}";
                    var cur = _state.GetOrAdd(key, _ => (last: false, stable: 0, lastSent: DateTime.MinValue, armed: true));

                    // Debounce
                    if (bit == cur.last) cur.stable++;
                    else { cur.last = bit; cur.stable = 1; }

                    bool debouncedHigh =  cur.last && cur.stable >= debounceCycles;
                    bool debouncedLow  = !cur.last && cur.stable >= debounceCycles;

                    // Re-arm on falling edge (allow next 0→1 to send immediately)
                    if (debouncedLow)
                    {
                        cur.armed    = true;
                        cur.lastSent = DateTime.MinValue; // reset any rate limit on clear
                    }

                    // Fire on rising edge when armed (optional rate-limit)
                    bool rateOk = rateLimit <= TimeSpan.Zero || DateTime.UtcNow - cur.lastSent >= rateLimit;
                    if (debouncedHigh && cur.armed && rateOk)
                    {
                        var recipients = ResolveRecipients(tag, groups);
                        if (recipients.Length > 0)
                        {
                            var addr = tag.Type.Equals("DBX", StringComparison.OrdinalIgnoreCase)
                                ? $"DB{tag.Db}.DBX{tag.Byte}.{tag.Bit}"
                                : $"{tag.Type}:{tag.Byte}.{tag.Bit}";

                            var subject = $"[{plcCfg.Name}] {tag.Name} ACTIVE";
                            var body =
                            $@"{DateTime.Now:yyyy-MM-dd HH:mm:ss} — Alarm '{tag.Name}' became ACTIVE
                            Station: {plcCfg.Name}"; // IP: {plcCfg.Ip}
                            //Address: {addr}";
                            await SendEmailAsync(emailCfg, recipients, subject, body);
                        }

                        cur.lastSent = DateTime.UtcNow;
                        cur.armed = false; // disarm until we see a debounced clear
                    }

                    _state[key] = cur;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("PLC {plc} error: {msg}", plcCfg.Name, ex.Message);
                try { plc.Close(); } catch { /* ignore */ }
                await Task.Delay(2000, ct); // backoff on comm error
            }

            await Task.Delay(pollMs, ct);
        }
    }

    private static string[] ResolveRecipients(TagConfig tag, Dictionary<string, string[]> groups)
    {
        var list = new List<string>();
        if (tag.EmailTo     is { Length: > 0 }) list.AddRange(tag.EmailTo);
        if (tag.EmailGroups is { Length: > 0 })
            foreach (var g in tag.EmailGroups)
                if (groups.TryGetValue(g, out var members)) list.AddRange(members);
        return list.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }

    private static bool ReadBit(Plc plc, TagConfig tag)
    {
        string t = tag.Type.ToUpperInvariant();
        static bool Mask(byte[] b, int bit) => (b[0] & (1 << bit)) != 0;

        return t switch
        {
            "DBX" => Mask((byte[])plc.ReadBytes(DataType.DataBlock, tag.Db, tag.Byte, 1), tag.Bit),
            "IX"  => Mask((byte[])plc.ReadBytes(DataType.Input,        0,    tag.Byte, 1), tag.Bit),
            "QX"  => Mask((byte[])plc.ReadBytes(DataType.Output,       0,    tag.Byte, 1), tag.Bit),
            "MX"  => Mask((byte[])plc.ReadBytes(DataType.Memory,       0,    tag.Byte, 1), tag.Bit),
            _     => throw new NotSupportedException($"Unsupported tag type '{tag.Type}'. Use DBX, IX, QX, or MX.")
        };
    }

    private static async Task SendEmailAsync(EmailConfig cfg, string[] recipients, string subject, string body)
    {
        if (recipients.Length == 0) return;

        var msg = new MimeMessage();
        msg.From.Add(new MailboxAddress(cfg.FromName, cfg.FromAddress));
        foreach (var r in recipients) msg.To.Add(MailboxAddress.Parse(r));
        msg.Subject = subject;
        msg.Body    = new TextPart("plain") { Text = body };

        // Accept env var OR inline value in PasswordSecretName. Strip spaces for Gmail app passwords.
        var candidate = Environment.GetEnvironmentVariable(cfg.PasswordSecretName);
        var password  = (candidate ?? cfg.PasswordSecretName ?? string.Empty).Replace(" ", "");

        using var smtp = new SmtpClient();
        await smtp.ConnectAsync(cfg.SmtpHost, cfg.SmtpPort, MailKit.Security.SecureSocketOptions.StartTls);
        await smtp.AuthenticateAsync(cfg.User, password);
        await smtp.SendAsync(msg);
        await smtp.DisconnectAsync(true);
    }
}

// ===== Models (safe defaults + groups) =====

public class PlcConfig
{
    public string Name { get; set; } = null!;
    public string CpuType { get; set; } = null!;
    public string Ip { get; set; } = null!;
    public short Rack { get; set; }
    public short Slot { get; set; }
    public List<TagConfig> Tags { get; set; } = new();
}

public class TagConfig
{
    public string Name { get; set; } = null!;
    public string Type { get; set; } = null!;   // "DBX" | "IX" | "QX" | "MX"
    public int Db { get; set; }                 // ignored for IX/QX/MX
    public int Byte { get; set; }
    public int Bit { get; set; }
    public string[] EmailTo { get; set; } = Array.Empty<string>();
    public string[] EmailGroups { get; set; } = Array.Empty<string>(); // optional recipient groups
}

public class EmailConfig
{
    public string SmtpHost { get; set; } = null!;
    public int SmtpPort { get; set; }
    public string User { get; set; } = null!;
    public string PasswordSecretName { get; set; } = null!; // env var name OR inline password
    public string FromName { get; set; } = null!;
    public string FromAddress { get; set; } = null!;
}
