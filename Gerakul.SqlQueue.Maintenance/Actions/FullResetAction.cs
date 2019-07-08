using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Maintenance.Actions
{
    public class FullResetAction : IAction
    {
        private const string ConnectionStringOption = "connectionString";
        private const string QueueOption = "queue";
        private const string SchemaOnlyDurabilityOption = "schemaOnlyDurability";

        public Task Execute(string[] args)
        {
            var options = ActionHelper.ParseOptions(args, 1);

            var connectionString = options.GetString(ConnectionStringOption);
            var queueName = options.GetString(QueueOption);
            var hasSchemaOnlyDurability = options.TryGetBool(SchemaOnlyDurabilityOption, out var schemaOnlyDurability);

            var maintenance = new Gerakul.SqlQueue.InMemory.Maintenance(connectionString, queueName);
            maintenance.FullReset(hasSchemaOnlyDurability ? schemaOnlyDurability : (bool?)null);
            return Task.CompletedTask;
        }
    }
}
