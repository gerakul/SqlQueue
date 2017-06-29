using Gerakul.SqlQueue.InMemory;
using System;

namespace Samples
{
    class Program
    {
        static void Main(string[] args)
        {
            // Note! Database must be configured for memory optimized tables before queue creation

            string connectionString = "{your connection string}";

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

            // deleting subscription
            client.DeleteSubscription("MySubscription");

            // deleting queue
            factory.DeleteQueue("MyQueue");
        }
    }
}