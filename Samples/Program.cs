using Gerakul.SqlQueue.Core;
using Gerakul.SqlQueue.InMemory;
using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Samples
{
    class Program
    {
        private static string connectionString = "{your connection string}";

        static void Main(string[] args)
        {
            // Note! Database must be configured for memory optimized tables before queue creation

            // creating queue
            var factory = new QueueFactory(connectionString);
            factory.CreateQueue("MyQueue");

            // connecting to queue
            var client = QueueClient.Create(connectionString, "MyQueue");

            // creating subscription
            client.CreateSubscription("MySubscription");

            // creating writer
            var writer = client.CreateWriter();

            // writing message to queue
            byte[] message = { 0x01, 0x02, 0x03 };
            var id = writer.Write(message);

            // creating reader for subscription
            var reader = client.CreateReader("MySubscription");

            // reading 1000 messages from subscription
            var messages = reader.Read(1000);

            // making massages completed after handling
            reader.Complete();

            // another way to handle messages - using AutoReader
            AutoReading().Wait();

            // deleting subscription
            client.DeleteSubscription("MySubscription");

            // deleting queue
            factory.DeleteQueue("MyQueue");

            Console.ReadKey();
        }

        private static async Task AutoReading()
        {
            // another way to handle messages - using AutoReader

            var client = QueueClient.Create(connectionString, "MyQueue");
            var writer = client.CreateWriter();

            byte[] message1 = { 0x01, 0x02, 0x03 };
            writer.Write(message1);

            byte[] message2 = { 0x04, 0x05, 0x06 };
            writer.Write(message2);


            // reading
            var autoReader = client.CreateAutoReader("MySubscription");
            await autoReader.Start(Handler);

            await Task.Delay(1000);

            await autoReader.Stop();
        }

        private static Task Handler(Message[] messages)
        {
            // handling messages
            Console.WriteLine($"Number:{messages.Length}");
            return Task.CompletedTask;
        }
    }
}