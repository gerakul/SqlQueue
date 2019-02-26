using Gerakul.FastSql.Common;
using Gerakul.SqlQueue.Core;
using Gerakul.SqlQueue.InMemory;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Maintenance
{
    public class ImportAction : IAction
    {
        private const string ConnectionStringOption = "connectionString";
        private const string FileOption = "file";
        private const string SubscriptionsOption = "subscriptions";
        private const string ReplaceOption = "replace";

        public Task Execute(string[] args)
        {
            var options = ActionHelper.ParseOptions(args, 1);

            if (!options.TryGetValue(ConnectionStringOption.ToLowerInvariant(), out var connectionString))
            {
                Console.WriteLine($"{ConnectionStringOption} is not found");
                return Task.CompletedTask;
            }

            if (!options.TryGetValue(FileOption.ToLowerInvariant(), out var file))
            {
                Console.WriteLine($"{FileOption} is not found");
                return Task.CompletedTask;
            }

            bool includeSubscriptions = options.ContainsKey(SubscriptionsOption.ToLowerInvariant());
            bool replace = options.ContainsKey(ReplaceOption.ToLowerInvariant());

            var json = File.ReadAllText(file);
            var queueConfList = JsonConvert.DeserializeObject<QueueConfigurationList>(json);

            var queueFactory = new QueueFactory(connectionString);

            foreach (var queueConf in queueConfList.Queues)
            {
                var toCreate = false;

                Console.WriteLine($"Started with queue {queueConf.Name}");

                if (queueFactory.IsQueueExsists(queueConf.Name))
                {
                    if (replace)
                    {
                        Console.Write($"Deleting queue {queueConf.Name}... ");
                        queueFactory.DeleteQueue(queueConf.Name);
                        Console.WriteLine($"Deleted");
                        toCreate = true;
                    }
                }
                else
                {
                    toCreate = true;
                }

                if (toCreate)
                {
                    Console.Write($"Creating queue {queueConf.Name}... ");
                    var queueClient = queueFactory.CreateQueue(queueConf.Name, queueConf.MinNum, queueConf.TresholdNum);
                    Console.WriteLine($"Created");

                    if (includeSubscriptions && queueConf.Subscriptions != null)
                    {
                        foreach (var subConf in queueConf.Subscriptions)
                        {
                            Console.Write($"Creating subscription {subConf.Name}... ");

                            queueClient.CreateSubscription(subConf.Name, new SubscriptionSettings()
                            {
                                ActionOnLimitExceeding = (ActionsOnLimitExceeding?)subConf.ActionOnLimitExceeding,
                                MaxIdleIntervalSeconds = subConf.MaxIdleIntervalSeconds,
                                MaxUncompletedMessages = subConf.MaxUncompletedMessages
                            });

                            if (subConf.Disabled)
                            {
                                queueClient.DisableSubscription(subConf.Name);
                            }

                            Console.WriteLine($"Created");
                        }
                    }
                }

                Console.WriteLine($"Finished with queue {queueConf.Name}");
            }

            return Task.CompletedTask;
        }
    }
}
