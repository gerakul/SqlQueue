# SqlQueue
Queue based on Sql Server Database

## Usage

Note! Database must be configured for memory optimized tables before queue creation

  ```csharp
        string connectionString = "{your connection string}";

        // creating queue
        var factory = new QueueFactory(connectionString);
        factory.CreateQueue("MyQueue");

        // connecting to queue
        var client = QueueClient.Create(connectionString, "MyQueue");

        // creating subscription
        client.CreateSubscription("MySubscription");

        // creating subscription with settings
        client.CreateSubscription("ProtectedSubscription", new SubscriptionSettings()
        {
            // take action if difference between last write and last complete exceeded MaxIdleIntervalSeconds
            MaxIdleIntervalSeconds = 3600,
            // take action if number of uncompleted messages exceeded MaxUncompletedMessages
            MaxUncompletedMessages = 200000,
            // action to take
            ActionOnLimitExceeding = ActionsOnLimitExceeding.DeleteSubscription
        });

        // update subscription settings
        client.UpdateSubscription("ProtectedSubscription", new SubscriptionSettings()
        {
            MaxIdleIntervalSeconds = 7200,
            MaxUncompletedMessages = 300000,
            ActionOnLimitExceeding = ActionsOnLimitExceeding.DisableSubscription
        });

        // retrieving information about subscription
        var info = client.GetSubscriptionInfo("MySubscription");

        // retrieving information about all subscriptions
        var allInfo = client.GetAllSubscriptionsInfo().ToArray();

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

        // deleting subscription
        client.DeleteSubscription("MySubscription");

        // deleting queue
        factory.DeleteQueue("MyQueue");
  ```  
  
  Another way to handle messages - using AutoReader
  
  ```csharp
        // creating AutoReader         
        var autoReader = client.CreateAutoReader("MySubscription");
        
        // start reading and handling
        await autoReader.Start(Handler /*delegate for processing of messages*/);
        
        // stop reading
        await autoReader.Stop();

		autoReader.Close();
  ```     
