using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using rebus.messages;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Config.Outbox;
using Rebus.Kafka;
using Rebus.Pipeline;
using Rebus.Pipeline.Send;
using Rebus.Routing.TypeBased;
using Rebus.Transport;

const string connectionString = "Data Source=(localdb)\\ProjectModels; Initial Catalog = master; Integrated Security = True; Connect Timeout = 30; Encrypt = False; TrustServerCertificate = False; ApplicationIntent = ReadWrite; MultiSubnetFailover = False";

var services = new ServiceCollection();

// https://github.com/glazkovalex/Rebus.Kafka
var producerSetupConfig = new ProducerConfig
{
    //BootstrapServers = , //will be set from the general parameter
    ApiVersionRequest = true,
    QueueBufferingMaxKbytes = 10240,
#if DEBUG
    Debug = "msg",
#endif
    MessageTimeoutMs = 3000,
    BootstrapServers = "127.0.0.100:9092"

};
producerSetupConfig.Set("request.required.acks", "-1");
producerSetupConfig.Set("queue.buffering.max.ms", "5");

using var adminClient = new AdminClientBuilder(producerSetupConfig).Build();

try
{
    await adminClient.DeleteTopicsAsync( new[] {
        "producer.input", "consumer.input"
    });

    await adminClient.CreateTopicsAsync(new[]
    {
    new TopicSpecification
    {
        Name = "producer.input",
        NumPartitions = 2,
    },
    new TopicSpecification
    {
        Name = "consumer.input",
        NumPartitions = 2,
    },
});
}
catch
{
    Console.WriteLine("Topics already exist");
}

var producerConfig = new ProducerConfig
{
    //BootstrapServers = , //will be set from the general parameter
    ApiVersionRequest = true,
    QueueBufferingMaxKbytes = 10240,
#if DEBUG
    Debug = "msg",
#endif
    MessageTimeoutMs = 3000,

};
producerConfig.Set("request.required.acks", "-1");
producerConfig.Set("queue.buffering.max.ms", "5");

var consumerConfig = new ConsumerConfig
{
    //BootstrapServers = , //will be set from the general parameter
    ApiVersionRequest = true,
    //GroupId = // will be set random
    EnableAutoCommit = false,
    FetchWaitMaxMs = 5,
    FetchErrorBackoffMs = 5,
    QueuedMinMessages = 1000,
    SessionTimeoutMs = 6000,
    //StatisticsIntervalMs = 5000,
#if DEBUG
    TopicMetadataRefreshIntervalMs = 20000, // Otherwise it runs maybe five minutes
    Debug = "msg",
#endif
    AutoOffsetReset = AutoOffsetReset.Latest,
    EnablePartitionEof = true,
    AllowAutoCreateTopics = true,
};
consumerConfig.Set("fetch.message.max.bytes", "10240");




// https://github.com/rebus-org/RebusSamples/tree/master/RequestReply
// https://github.com/rebus-org/RebusSamples/blob/master/RequestReply/Producer/Program.cs
services.AddRebus(
    configure => configure
    // .Logging(...)
    .Transport(t => t.UseKafka("127.0.0.100:9092", "producer.input", producerConfig, consumerConfig))
    //.Transport(t => t.UseSqlServer(connectionString, "producer.input"))
    .Outbox(o => o.StoreInSqlServer(connectionString, "producer.outbox"))
    .Routing(r => r.TypeBased().MapAssemblyOf<TestMessage>("consumer.input"))
    //.Routing(...)
    .Options(o =>
    {
        o.AddCustomExtension();
        o.LogPipeline(verbose: true);
    })
    );

// https://thecloudblog.net/post/building-reliable-kafka-producers-and-consumers-in-net/
// https://dzone.com/articles/custom-partitioner-in-kafka-lets-take-quick-tour (Java but nicely explained)
// Create message producer and add a custom partitioner
using var producer = new ProducerBuilder<long, string>(producerConfig)
    .SetKeySerializer(Serializers.Int64)
    .SetValueSerializer(Serializers.Utf8)
    .SetLogHandler((_, message) =>
        Console.WriteLine($"Facility: {message.Facility}-{message.Level} Message: {message.Message}"))
    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}. Is Fatal: {e.IsFatal}"))
    .SetPartitioner("consumer.input", (string topic, int partitionCount, ReadOnlySpan<byte> keyData, bool keyIsNull) =>
    {
        return 1;
    })
    .Build();



using (var provider = services.BuildServiceProvider())
{
    provider.UseRebus();

    var bus = provider.GetRequiredService<IBus>();

    do
    {

        // https://github.com/rebus-org/Rebus/issues/819#issuecomment-1118565853
        using var connection = new SqlConnection(connectionString);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        try
        {
            using var scope = new RebusTransactionScope();

            scope.UseOutbox(connection, transaction);





            // Execute actual code and produce messages to send
            await bus.Send(new TestMessage()
            {
                Id = 1,
                Message = "Testmessage"
            });



            // completing the scope will insert outgoing messages using the connection/transaction
            await scope.CompleteAsync();

            // commit all the things! 👍 
            await transaction.CommitAsync();


            // Asynchronously send a message to a specific partition
            //var deliveryReport = await producer.ProduceAsync("consumer.input",
            //    new Message<long, string>
            //    {
            //        Key = DateTime.UtcNow.Ticks,
            //        Value = "Hallo"
            //    });
            //producer.Flush();
        }
        catch (Exception exception)
        {
            Console.WriteLine(exception.ToString());    
        }





        Console.WriteLine("Press any key to send the next message");
        _ = Console.ReadKey();
    } while (true);


}

public static class RebusExtensions
{
    public static void AddCustomExtension(this OptionsConfigurer configurer)
    {
        configurer.Decorate<IPipeline>(c =>
        {
            var pipeline = c.Get<IPipeline>();
            var step = new CustomExtensionStep();
            return new PipelineStepInjector(pipeline)
                .OnSend(step, PipelineRelativePosition.Before, typeof(SerializeOutgoingMessageStep));
        });
    }
}

[StepDocumentation("Custom extension step that does something when sending a message")]
public class CustomExtensionStep : IOutgoingStep
{
    public async Task Process(OutgoingStepContext context, Func<Task> next)
    {

        var message = context.Load<TestMessage>();

        Console.WriteLine("CustomExtensionStep");

        await next();
    }
}


