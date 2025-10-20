using Microsoft.Extensions.Hosting;

namespace PlcAlarmNotifier;

public class Program
{
    public static void Main(string[] args)
    {
        CreateHostBuilder(args).Build().Run();
    }

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            // Uncomment next line when you want to run as a Windows Service:
            // .UseWindowsService()
            .ConfigureServices((context, services) =>
            {
                services.AddHostedService<Worker>();
            });
}
