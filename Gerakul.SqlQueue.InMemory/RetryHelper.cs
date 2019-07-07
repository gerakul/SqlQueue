using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Linq;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.InMemory
{
    internal static class RetryHelper
    {
        // https://docs.microsoft.com/En-us/sql/database-engine/guidelines-for-retry-logic-for-transactions-on-memory-optimized-tables
        private static readonly int[] errorCodes = { 41301, 41302, 41305, 41325 };

        private static readonly int[] forceCleanInActionCode = { 50005 };

        internal static T Retry<T>(Func<T> action, int count)
        {
            if (count <= 1)
            {
                return action();
            }

            int counter = 1;
            while (true)
            {
                try
                {
                    return action();
                }
                catch (SqlException ex) when (ExceptionCondition(ex, errorCodes))
                {
                    if (counter >= count)
                    {
                        throw;
                    }

                    counter++;
                    // this delay significantly decreases number of conflicts
                    Task.Delay(1).GetAwaiter().GetResult();
                }
                catch (SqlException ex) when (ExceptionCondition(ex, forceCleanInActionCode))
                {
                    if (counter >= count)
                    {
                        throw;
                    }

                    counter++;
                    Task.Delay(100).GetAwaiter().GetResult();
                }
            }
        }

        private static bool ExceptionCondition(SqlException ex, int[] codes)
        {
            foreach (var error in ex.Errors)
            {
                if (error is SqlError)
                {
                    if (!codes.Contains(((SqlError)error).Number))
                    {
                        return false;
                    }
                }
                else
                {
                    return false;
                }
            }

            return true;
        }
    }
}
