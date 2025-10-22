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
    private readonly ConcurrentDictionary<string, (bool last, int stable, DateTime lastSent, bool armed)> _tagState = new();

    // PLC health: online flag, consecutive failure count, last notification timestamp
    private readonly ConcurrentDictionary<string, (bool online, int failCount, DateTime lastNotify)> _plcHealth = new();

    public Worker(ILogger<Worker> logger, IConfiguration config)
    {
        _logger = logger;
        _config = config;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _logger.LogInformation("PLC Alarm Notifier starting...");

        var plcs            = _config.GetSection("Plcs").Get<List<PlcConfig>>() ?? new();
        var emailCfg        = _config.GetSection("Email").Get<EmailConfig>()     ?? throw new InvalidOperationException("Missing Email config");
        var recipientGroups = _config.GetSection("RecipientGroups").Get<Dictionary<string, string[]>>() ?? new();

        int pollMs         = _config.GetValue<int>("PollingMs", 750);
        int debounceCycles = _config.GetValue<int>("DebounceCycles", 2);
        var rateLimit      = TimeSpan.FromMinutes(_config.GetValue<int>("RateLimitMinutes", 0)); // 0 = disabled

        // Communications alarm settings
        bool commNotify     = _config.GetValue<bool>("CommLost:Notify", true);
        bool restoreNotify  = _config.GetValue<bool>("CommLost:RestoreNotify", true);
        int  commDebounce   = _config.GetValue<int>("CommLost:DebounceFailures", 3);
        var  commReminder   = TimeSpan.FromMinutes(_config.GetValue<int>("CommLost:ReminderMinutes", 60));
        var  commGroups     = _config.GetSection("CommLost:EmailGroups").Get<string[]>() ?? Array.Empty<string>();

        // Start one poller per PLC (staggered)
        var tasks = plcs.Select(async plcCfg =>
        {
            await Task.Delay(Random.Shared.Next(0, 1000), ct);
            await PollPlcLoop(plcCfg, recipientGroups, emailCfg, pollMs, debounceCycles, rateLimit, ct,
                              commNotify, restoreNotify, commDebounce, commReminder, commGroups);
        });

        await Task.WhenAll(tasks);
    }

    private async Task PollPlcLoop(
        PlcConfig plcCfg,
        Dictionary<string, string[]> groups,
        EmailConfig emailCfg,
        int pollMs,
        int debounceCycles,
        TimeSpan rateLimit,
        CancellationToken ct,
        bool commNotify,
        bool restoreNotify,
        int commDebounce,
        TimeSpan commReminder,
        string[] commGroups)
    {
        var cpu = Enum.Parse<CpuType>(plcCfg.CpuType, ignoreCase: true);
        using var plc = new Plc(cpu, plcCfg.Ip, plcCfg.Rack, plcCfg.Slot);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (!plc.IsConnected) plc.Open();

                // If we just recovered, mark ONLINE and optionally notify
                var health = _plcHealth.GetOrAdd(plcCfg.Name, _ => (online: true, failCount: 0, lastNotify: DateTime.MinValue));
                if (!health.online)
                {
                    _plcHealth[plcCfg.Name] = (true, 0, health.lastNotify);
                    if (commNotify && restoreNotify)
                    {
                        var recipients = ResolveGroupOnly(groups, commGroups);
                        if (recipients.Length > 0)
                        {
                            var subject = $"[{plcCfg.Name}] Communications RESTORED";
                            var body    = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} — PLC communications restored.\nStation: {plcCfg.Name}";
                            await SendEmailAsync(emailCfg, recipients, subject, body);
                        }
                    }
                    _logger.LogInformation("PLC {plc} communications restored.", plcCfg.Name);
                }

                // ---------- Poll tags (Rising-edge with re-arm on falling edge) ----------
                foreach (var tag in plcCfg.Tags)
                {
                    bool bit = ReadBit(plc, tag);

                    string key = $"{plcCfg.Name}.{tag.Name}";
                    var cur = _tagState.GetOrAdd(key, _ => (last: false, stable: 0, lastSent: DateTime.MinValue, armed: true));

                    // Debounce sample
                    if (bit == cur.last) cur.stable++;
                    else { cur.last = bit; cur.stable = 1; }

                    bool debouncedHigh =  cur.last && cur.stable >= debounceCycles; // 1 sustained
                    bool debouncedLow  = !cur.last && cur.stable >= debounceCycles; // 0 sustained

                    // Re-arm on falling edge (allow next 0→1 to send immediately)
                    if (debouncedLow)
                    {
                        cur.armed    = true;
                        cur.lastSent = DateTime.MinValue; // clear any rate-limit on clear
                    }

                    // Fire on rising edge when armed (optional rate-limit)
                    bool rateOk = rateLimit <= TimeSpan.Zero || DateTime.UtcNow - cur.lastSent >= rateLimit;
                    if (debouncedHigh && cur.armed && rateOk)
                    {
                        var recipients = ResolveRecipients(tag, groups);
                        if (recipients.Length > 0)
                        {
                            var subject = $"[{plcCfg.Name}] {tag.Name} ACTIVE";
                            var body =
$@"{DateTime.Now:yyyy-MM-dd HH:mm:ss} — Alarm '{tag.Name}' became ACTIVE
Station: {plcCfg.Name}";
                            await SendEmailAsync(emailCfg, recipients, subject, body);
                        }

                        cur.lastSent = DateTime.UtcNow;
                        cur.armed = false; // wait for clear
                    }

                    _tagState[key] = cur;
                }
                // ------------------------------------------------------------------------
            }
            catch (Exception ex)
            {
                // Update and evaluate health as OFFLINE
                var cur = _plcHealth.AddOrUpdate(
                    plcCfg.Name,
                    _ => (online: false, failCount: 1, lastNotify: DateTime.MinValue),
                    (_, prev) => (online: false, failCount: prev.failCount + 1, lastNotify: prev.lastNotify));

                // Notify "Comms LOST" after N consecutive failures, then remind every commReminder
                if (commNotify && cur.failCount >= commDebounce)
                {
                    var now = DateTime.UtcNow;
                    if (cur.lastNotify == DateTime.MinValue || now - cur.lastNotify >= commReminder)
                    {
                        var recipients = ResolveGroupOnly(groups, commGroups);
                        if (recipients.Length > 0)
                        {
                            var subject = $"[{plcCfg.Name}] Communications LOST";
                            var body    = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} — PLC unreachable.\nStation: {plcCfg.Name}\nError: {ex.Message}";
                            await SendEmailAsync(emailCfg, recipients, subject, body);
                        }
                        _plcHealth[plcCfg.Name] = (false, cur.failCount, now);
                    }
                }

                _logger.LogError("PLC {plc} error: {msg}", plcCfg.Name, ex.Message);
                try { plc.Close(); } catch { /* ignore */ }
                await Task.Delay(2000, ct); // brief backoff
                // skip tag loop this cycle
            }

            await Task.Delay(pollMs, ct);
        }
    }

    private static string[] ResolveRecipients(TagConfig tag, Dictionary<string, string[]> groups)
    {
        var list = new List<string>();
        if (tag.EmailTo     is { Length: > 0 }) list.AddRange(tag.EmailTo);
        if (tag.EmailGroups is { Length: > 0 })
        {
            foreach (var g in tag.EmailGroups)
                if (groups.TryGetValue(g, out var members)) list.AddRange(members);
        }
        return list.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }

    private static string[] ResolveGroupOnly(Dictionary<string, string[]> groups, string[] groupNames)
    {
        var list = new List<string>();
        foreach (var g in groupNames)
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

// ===== Models =====

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
