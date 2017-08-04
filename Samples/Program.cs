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
            factory.DeleteQueue("MyQueue");
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

            // writing batch of messages to queue
            byte[][] batch = {
                new byte[] { 0x01, 0x02, 0x03 },
                new byte[] { 0x01, 0x02, 0x04 },
                new byte[] { 0x01, 0x02, 0x05 },
            };

            var ids = writer.WriteMany(batch, true);

            writer.Close();

            // creating reader for subscription
            var reader = client.CreateReader("MySubscription");

            // reading 1000 messages from subscription
            var messages = reader.Read(1000);

            // making massages completed after handling
            reader.Complete();

            reader.Close();

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

            byte[][] batch = {
                new byte[] { 0x01, 0x02, 0x03 },
                new byte[] { 0x04, 0x05, 0x06 },
                new byte[] { 0x07, 0x08, 0x09 },
            };

            writer.WriteMany(batch);

            writer.Close();

            // reading
            var autoReader = client.CreateAutoReader("MySubscription");
            await autoReader.Start(Handler);

            await Task.Delay(1000);

            await autoReader.Stop();

            autoReader.Close();
        }

        private static Task Handler(Message[] messages)
        {
            // handling messages
            Console.WriteLine($"Number:{messages.Length}");
            return Task.CompletedTask;
        }
    }
}