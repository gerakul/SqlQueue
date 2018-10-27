using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Text.RegularExpressions;

namespace Gerakul.SqlQueue.InMemory
{
    internal static class Helper
    {
        internal static void ExecuteBatches(SqlConnection conn, string text)
        {
            foreach (var item in GetSqlBatches(text))
            {
                if (!string.IsNullOrWhiteSpace(item))
                {
                    new SqlCommand(item, conn).ExecuteNonQuery();
                }
            }
        }

        internal static string[] GetSqlBatches(string text)
        {
            return Regex.Split(text, @"^\s*go\s*$", RegexOptions.IgnoreCase | RegexOptions.Multiline);
        }
    }
}
