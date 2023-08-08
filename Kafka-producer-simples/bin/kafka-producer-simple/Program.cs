using Confluent.Kafka;

namespace kafka_producer_simple;

static class Program
{
    public static async Task Main()
    {
        const string _topic = "kafka-simple-topic";
        const string _adressService = "127.0.0.1:9092";

        Message<Null, string> _message = new()
        {
            Value = "Olá mundo!"
        };

        var config = new ProducerConfig { BootstrapServers = _adressService };
        using var producerBuilder = new ProducerBuilder<Null, string>(config).Build();

        try
        {
            while (true)
            {
                var dr = await producerBuilder.ProduceAsync(_topic, _message);

                Console.WriteLine($"Entregue: Messagem => {dr.Value} Tópico: {dr.Topic}");

                Thread.Sleep(9000);
            }
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
        }

    }
}