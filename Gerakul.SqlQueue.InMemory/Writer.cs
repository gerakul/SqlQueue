using Gerakul.SqlQueue.Core;
using System;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.InMemory
{
    public class Writer : IWriter
    {
        private QueueClient queueClient;
        private SqlConnection connection;
        private SqlCommand writeCommand;
        private bool needReconnect = true;
        private int cleanMinIntervalSeconds;
        private DateTime lastCleanup;
        private object lockObj = new object();

        internal Writer(QueueClient queueClient, int cleanMinIntervalSeconds)
        {
            this.queueClient = queueClient;
            this.cleanMinIntervalSeconds = cleanMinIntervalSeconds;
        }

        private void Reconnect()
        {
            if (connection != null && (connection.State & System.Data.ConnectionState.Open) == System.Data.ConnectionState.Open)
            {
                connection.Close();
            }

            connection = new SqlConnection(queueClient.ConnectionString);
            connection.Open();

            writeCommand = connection.CreateCommand();
            writeCommand.CommandType = System.Data.CommandType.StoredProcedure;
            writeCommand.CommandText = $"[{queueClient.QueueName}].[Write]";
            writeCommand.Parameters.Add("body", System.Data.SqlDbType.Binary);
            writeCommand.Parameters.Add(new SqlParameter("id", System.Data.SqlDbType.BigInt) { Direction = System.Data.ParameterDirection.Output });
            writeCommand.Prepare();

            needReconnect = false;
        }

        private void Clean()
        {
            using (SqlConnection conn = new SqlConnection(queueClient.ConnectionString))
            {
                conn.Open();

                var cmd = connection.CreateCommand();
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.CommandText = $"[{queueClient.QueueName}].[Clean]";
                cmd.ExecuteNonQuery();
            }
        }

        public long Write(byte[] data)
        {
            long id;
            lock (lockObj)
            {
                if (needReconnect)
                {
                    Reconnect();
                }

                try
                {
                    writeCommand.Parameters[0].Value = data;
                    writeCommand.ExecuteNonQuery();

                    id = (long)writeCommand.Parameters[1].Value;
                }
                catch
                {
                    needReconnect = true;
                    throw;
                }
            }

            if ((DateTime.UtcNow - lastCleanup).TotalSeconds > cleanMinIntervalSeconds)
            {
                Clean();
                lastCleanup = DateTime.UtcNow;
            }

            return id;
        }
    }
}
