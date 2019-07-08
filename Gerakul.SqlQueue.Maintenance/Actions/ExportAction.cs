using Gerakul.FastSql.Common;
using Gerakul.FastSql.SqlServer;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Maintenance.Actions
{
    public class ExportAction : IAction
    {
        private const string ConnectionStringOption = "connectionString";
        private const string FileOption = "file";
        private const string SubscriptionsOption = "subscriptions";

        public async Task Execute(string[] args)
        {
            var options = ActionHelper.ParseOptions(args, 1);

            var connectionString = options.GetString(ConnectionStringOption);
            var file = options.GetString(FileOption);
            bool includeSubscriptions = options.IsExists(SubscriptionsOption);

            var dbContext = SqlContextProvider.DefaultInstance.CreateContext(connectionString);

            var queueList = await dbContext.CreateSimple(@"
                select s.name
                from sys.tables t
                    join sys.schemas s on t.schema_id = s.schema_id
                where t.name in ('Global', 'Messages0', 'Messages1', 'Messages2', 'Settings', 'State', 'Subscription')
                group by s.name
                having count(*) = 7").ExecuteQueryFirstColumnAsync<string>().ToArray();

            List<QueueConfiguration> queues = new List<QueueConfiguration>();
            foreach (var queueName in queueList)
            {
                var queueConf = await dbContext.CreateSimple($@"
                    select '{queueName}' as [Name], MinNum, TresholdNum
                    from [{queueName}].[Settings]").ExecuteQueryAsync<QueueConfiguration>().FirstOrDefault();

                if (includeSubscriptions)
                {
                    queueConf.Subscriptions = await dbContext.CreateSimple($@"
                        select [Name], [Disabled], MaxIdleIntervalSeconds, MaxUncompletedMessages, ActionOnLimitExceeding
                        from [{queueName}].[Subscription]").ExecuteQueryAsync<SubscriptionConfiguration>().ToList();
                }

                queues.Add(queueConf);
            }

            var queueConfList = new QueueConfigurationList() { Queues = queues };

            var json = JsonConvert.SerializeObject(queueConfList, Formatting.Indented);

            File.WriteAllText(file, json);
        }
    }
}
