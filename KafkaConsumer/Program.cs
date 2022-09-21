using CommonLib;
using Confluent.Kafka;
using Newtonsoft.Json;

var config = new ConsumerConfig
{
    GroupId = "Kafka-comsumer-group",
    BootstrapServers = "localhost:9092",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<Null, string>(config).Build();

consumer.Subscribe("my-kafka-topic");

CancellationTokenSource token = new();

try
{
    while(true)
    {
        var response = consumer.Consume(token.Token);
        if (response.Message != null)
        {
            var kafkaMsg = JsonConvert.DeserializeObject<KafkaMessage>(response.Message!.Value);
            Console.WriteLine($"Message : {kafkaMsg!.State}  {kafkaMsg.Value} ");
        }
    }
}
catch (ConsumeException ex)
{
    Console.WriteLine(ex.Message);
}