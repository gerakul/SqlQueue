using Gerakul.SqlQueue.Core;
using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.InMemory
{
    public class Writer : IWriter, IWriterMany
    {
        private QueueClient queueClient;
        private SqlConnection connection;
        private SqlCommand writeCommand;
        private SqlCommand writeManyCommand;
        private bool needReconnect = true;
        private int cleanMinIntervalSeconds;
        private DateTime lastCleanup;
        private object lockObj = new object();
        private int cleaning = 0;

        private SqlMetaData[] schema = new SqlMetaData[] {
                        new SqlMetaData("ID", System.Data.SqlDbType.Int),
                        new SqlMetaData("Body", System.Data.SqlDbType.VarBinary, 8000),
                    };

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

            SqlConnectionStringBuilder csb = new SqlConnectionStringBuilder(queueClient.ConnectionString);
            if (csb.Pooling)
            {
                csb.Pooling = false;
            }

            connection = new SqlConnection(csb.ConnectionString);
            connection.Open();

            writeCommand = connection.CreateCommand();
            writeCommand.CommandType = System.Data.CommandType.StoredProcedure;
            writeCommand.CommandText = $"[{queueClient.QueueName}].[Write]";
            writeCommand.Parameters.Add("body", System.Data.SqlDbType.Binary);
            writeCommand.Parameters.Add(new SqlParameter("id", System.Data.SqlDbType.BigInt) { Direction = System.Data.ParameterDirection.Output });
            writeCommand.Prepare();

            writeManyCommand = connection.CreateCommand();
            writeManyCommand.CommandType = System.Data.CommandType.StoredProcedure;
            writeManyCommand.CommandText = $"[{queueClient.QueueName}].[WriteMany]";
            writeManyCommand.Parameters.Add("messageList", System.Data.SqlDbType.Structured);
            writeManyCommand.Parameters.Add("returnIDs", System.Data.SqlDbType.Bit);
            writeManyCommand.Prepare();

            needReconnect = false;
        }

        private void Clean()
        {
            if (Interlocked.CompareExchange(ref cleaning, 1, 0) == 1)
            {
                return;
            }

            try
            {
                using (SqlConnection conn = new SqlConnection(queueClient.ConnectionString))
                {
                    conn.Open();

                    var cmd = conn.CreateCommand();
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;
                    cmd.CommandText = $"[{queueClient.QueueName}].[Clean]";
                    cmd.ExecuteNonQuery();
                }

                lastCleanup = DateTime.UtcNow;
            }
            finally
            {
                Interlocked.Exchange(ref cleaning, 0);
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
            }

            return id;
        }

        public long[] WriteMany(IEnumerable<byte[]> data, bool returnIDs = false)
        {
            List<long> ids = null;
            lock (lockObj)
            {
                if (needReconnect)
                {
                    Reconnect();
                }

                try
                {
                    //writeManyCommand.Parameters[0].Value = new MessDbDataReader(data); // not working in net46
                    writeManyCommand.Parameters[0].Value = GetRecords(data);
                    writeManyCommand.Parameters[1].Value = returnIDs;

                    if (returnIDs)
                    {
                        ids = new List<long>();
                        using (var r = writeManyCommand.ExecuteReader())
                        {
                            while (r.Read())
                            {
                                ids.Add(r.GetInt64(0));
                            }
                        }
                    }
                    else
                    {
                        writeManyCommand.ExecuteNonQuery();
                    }
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
            }

            return ids?.ToArray();
        }

        private IEnumerable<SqlDataRecord> GetRecords(IEnumerable<byte[]> data)
        {
            int n = 1;
            foreach (var item in data)
            {
                var rec = new SqlDataRecord(schema);
                rec.SetInt32(0, n++);
                rec.SetSqlBytes(1, new System.Data.SqlTypes.SqlBytes(item));
                yield return rec;
            }
        }
    }
}
