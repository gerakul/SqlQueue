using Gerakul.FastSql.Common;
using Gerakul.FastSql.SqlServer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Maintenance.Actions
{
    public class ForceCleanAction : IAction
    {
        private const string ConnectionStringOption = "connectionString";
        private const string QueueOption = "queue";

        public Task Execute(string[] args)
        {
            var options = ActionHelper.ParseOptions(args, 1);

            var connectionString = options.GetString(ConnectionStringOption);
            var queueName = options.GetString(QueueOption);

            var dbContext = SqlContextProvider.DefaultInstance.CreateContext(connectionString);
            return dbContext.CreateProcedureSimple($"[{queueName}].[ForceClean]").ExecuteNonQueryAsync();
        }
    }
}
