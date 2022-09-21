using CommonLib;
using Confluent.Kafka;

using Newtonsoft.Json;

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Acks = Acks.Leader
};

using var producer = new ProducerBuilder<Null, string>(config).Build();
try
{
    string? state;
    while ((state = Console.ReadLine()) != null)
    {
        var response = await producer.ProduceAsync(
            "my-kafka-topic",
            new Message<Null, string> { 
                Value = JsonConvert.SerializeObject(
                    new KafkaMessage(state,10)
                )});
    }

}
catch (ProduceException<Null, string> ex)
{
    Console.WriteLine(ex.Message);
}

