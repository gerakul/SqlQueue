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
    public sealed class Writer : IWriter, IWriterMany, IDisposable
    {
        public QueueClient QueueClient { get; }

        private SqlConnection connection;
        private SqlCommand writeCommand;
        private SqlCommand writeManyCommand;
        private bool needReconnect = true;
        private int cleanMinIntervalSeconds;
        private DateTime lastCleanup;
        private DateTime lastWrite;
        private object lockObj = new object();
        private int cleaning = 0;
        private Timer cleanTimer;

        public event EventHandler<CleanExceptionEventArgs> CleanException;

        private SqlMetaData[] schema = new SqlMetaData[] {
                        new SqlMetaData("ID", System.Data.SqlDbType.Int),
                        new SqlMetaData("Body", System.Data.SqlDbType.VarBinary, 8000),
                    };

        internal Writer(QueueClient queueClient, int cleanMinIntervalSeconds)
        {
            this.QueueClient = queueClient;
            this.cleanMinIntervalSeconds = cleanMinIntervalSeconds;
        }

        private void OnCleanException(CleanExceptionEventArgs e)
        {
            var handler = CleanException;
            handler?.Invoke(this, e);
        }

        private void Reconnect()
        {
            CloseResources();

            SqlConnectionStringBuilder csb = new SqlConnectionStringBuilder(QueueClient.ConnectionString);
            if (csb.Pooling)
            {
                csb.Pooling = false;
            }

            connection = new SqlConnection(csb.ConnectionString);
            connection.Open();

            writeCommand = connection.CreateCommand();
            writeCommand.CommandType = System.Data.CommandType.StoredProcedure;
            writeCommand.CommandText = $"[{QueueClient.QueueName}].[Write]";
            writeCommand.Parameters.Add("body", System.Data.SqlDbType.Binary);
            writeCommand.Parameters.Add(new SqlParameter("id", System.Data.SqlDbType.BigInt) { Direction = System.Data.ParameterDirection.Output });
            writeCommand.Prepare();

            writeManyCommand = connection.CreateCommand();
            writeManyCommand.CommandType = System.Data.CommandType.StoredProcedure;
            writeManyCommand.CommandText = $"[{QueueClient.QueueName}].[WriteMany]";
            writeManyCommand.Parameters.Add("messageList", System.Data.SqlDbType.Structured);
            writeManyCommand.Parameters.Add("returnIDs", System.Data.SqlDbType.Bit);
            writeManyCommand.Prepare();

            this.cleanTimer = new Timer(new TimerCallback(x => CleanIfNeed()), null, cleanMinIntervalSeconds * 60, cleanMinIntervalSeconds * 60);

            needReconnect = false;
        }

        private void CleanIfNeed()
        {
            if ((DateTime.UtcNow - lastCleanup).TotalSeconds <= cleanMinIntervalSeconds
                || (DateTime.UtcNow - lastWrite).TotalSeconds > cleanMinIntervalSeconds * 10)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref cleaning, 1, 0) == 1)
            {
                return;
            }

            try
            {
                using (SqlConnection conn = new SqlConnection(QueueClient.ConnectionString))
                {
                    conn.Open();

                    var cmd = conn.CreateCommand();
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;
                    cmd.CommandText = $"[{QueueClient.QueueName}].[Clean]";
                    cmd.ExecuteNonQuery();
                }

                lastCleanup = DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                OnCleanException(new CleanExceptionEventArgs(ex));
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

                lastWrite = DateTime.UtcNow;
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

                lastWrite = DateTime.UtcNow;
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

        private void CloseResources()
        {
            writeCommand?.Dispose();
            writeManyCommand?.Dispose();
            connection?.Close();
            cleanTimer?.Dispose();
        }

        public void Close()
        {
            CloseResources();
            GC.SuppressFinalize(this);
        }

        void IDisposable.Dispose()
        {
            Close();
        }
    }

    public class CleanExceptionEventArgs : EventArgs
    {
        public Exception Exception { get; private set; }

        public CleanExceptionEventArgs(Exception exception)
        {
            this.Exception = exception;
        }
    }
}
