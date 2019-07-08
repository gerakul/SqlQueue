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

namespace Gerakul.SqlQueue.Maintenance.Actions
{
    public class ImportAction : IAction
    {
        private const string ConnectionStringOption = "connectionString";
        private const string FileOption = "file";
        private const string SubscriptionsOption = "subscriptions";
        private const string ReplaceOption = "replace";

        private readonly IOutput output;

        public ImportAction(IOutput output)
        {
            this.output = output;
        }

        public Task Execute(string[] args)
        {
            var options = ActionHelper.ParseOptions(args, 1);

            var connectionString = options.GetString(ConnectionStringOption);
            var file = options.GetString(FileOption);
            bool includeSubscriptions = options.IsExists(SubscriptionsOption);
            bool replace = options.IsExists(ReplaceOption);

            var json = File.ReadAllText(file);
            var queueConfList = JsonConvert.DeserializeObject<QueueConfigurationList>(json);

            var queueFactory = new QueueFactory(connectionString);

            foreach (var queueConf in queueConfList.Queues)
            {
                var toCreate = false;

                output.WriteLine($"Started with queue {queueConf.Name}");

                if (queueFactory.IsQueueExsists(queueConf.Name))
                {
                    if (replace)
                    {
                        output.Write($"Deleting queue {queueConf.Name}... ");
                        queueFactory.DeleteQueue(queueConf.Name);
                        output.WriteLine($"Deleted");
                        toCreate = true;
                    }
                }
                else
                {
                    toCreate = true;
                }

                if (toCreate)
                {
                    output.Write($"Creating queue {queueConf.Name}... ");
                    var queueClient = queueFactory.CreateQueue(queueConf.Name, queueConf.MinNum, queueConf.TresholdNum);
                    output.WriteLine($"Created");

                    if (includeSubscriptions && queueConf.Subscriptions != null)
                    {
                        foreach (var subConf in queueConf.Subscriptions)
                        {
                            output.Write($"Creating subscription {subConf.Name}... ");

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

                            output.WriteLine($"Created");
                        }
                    }
                }

                output.WriteLine($"Finished with queue {queueConf.Name}");
            }

            return Task.CompletedTask;
        }
    }
}
