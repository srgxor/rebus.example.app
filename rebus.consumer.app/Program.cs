using Microsoft.Extensions.DependencyInjection;
using rebus.consumer.app;
using Rebus.Config;
using Rebus.Config.Outbox;
using Rebus.Kafka;

const string connectionString = "Data Source=(localdb)\\ProjectModels; Initial Catalog = master; Integrated Security = True; Connect Timeout = 30; Encrypt = False; TrustServerCertificate = False; ApplicationIntent = ReadWrite; MultiSubnetFailover = False";

var services = new ServiceCollection();

// https://github.com/rebus-org/RebusSamples/tree/master/RequestReply
// https://github.com/rebus-org/RebusSamples/blob/master/RequestReply/Consumer/Program.cs
services.AddRebus(
    configure => configure
    // .Logging(...)
    .Transport(t => t.UseKafka("127.0.0.100:9092", "consumer.input", "consumer"))
    //.Transport(t => t.UseSqlServer(connectionString, "consumer.input"))
    .Outbox(o => o.StoreInSqlServer(connectionString, "consumer.outbox"))
    //.Routing(...)
    //.Options(...)
    );


services.AutoRegisterHandlersFromAssemblyOf<TestMessageHandler>();


using (var provider = services.BuildServiceProvider())
{
    provider.UseRebus();

    Console.WriteLine("Press any key to exit");
    _ = Console.ReadKey();
}
