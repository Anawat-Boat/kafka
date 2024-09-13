using Confluent.Kafka;

namespace Kafka.BackgroundServices
{
    public class KafkaConsumerService : BackgroundService
    {
        private readonly ConsumerConfig _config;

        public KafkaConsumerService()
        {
            _config = new ConsumerConfig
            {
                GroupId = "file-consumer-group",
                BootstrapServers = "10.112.86.164:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using (var consumer = new ConsumerBuilder<Null, string>(_config).Build())
            {
                consumer.Subscribe("re-calculate");
                try
                {
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        var consumeResult = consumer.Consume(stoppingToken);
                        Console.WriteLine($"Consumed file content: {consumeResult.Message.Value}");

                        // Optionally, save the content to a file
                        await File.WriteAllTextAsync($"received-file-{Guid.NewGuid()}.txt", consumeResult.Message.Value);
                    }

                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error: {e.Error.Reason}");
                }
                finally
                {
                    consumer.Close();
                }
            }
            // await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
        }
    }
}