using Gerakul.SqlQueue.Core;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

namespace Gerakul.SqlQueue.InMemory
{
    public class Reader : IReader
    {
        public QueueClient QueueClient { get; }
        public string Subscription { get; }

        private SqlConnection connection;
        private SqlCommand readCommand;
        private SqlCommand completeCommand;
        private SqlCommand relockCommand;
        private SqlCommand unlockCommand;
        private bool needReconnect = true;
        private int subscriptionID = 0;
        private Guid currentLockToken = Guid.Empty;
        private long idToComplete = 0;
        private int defaultCheckLockSeconds;
        private object lockObj = new object();

        internal Reader(QueueClient queueClient, string subscription, int defaultCheckLockSeconds)
        {
            this.QueueClient = queueClient;
            this.Subscription = subscription;
            this.defaultCheckLockSeconds = defaultCheckLockSeconds;
        }

        private void Reconnect()
        {
            if (subscriptionID == 0)
            {
                subscriptionID = QueueClient.FindSubscriptionOrThrowException(Subscription);
            }

            if (connection != null && (connection.State & System.Data.ConnectionState.Open) == System.Data.ConnectionState.Open)
            {
                connection.Close();
            }

            SqlConnectionStringBuilder csb = new SqlConnectionStringBuilder(QueueClient.ConnectionString);
            if (csb.Pooling)
            {
                csb.Pooling = false;
            }

            connection = new SqlConnection(csb.ConnectionString);
            connection.Open();

            readCommand = connection.CreateCommand();
            readCommand.CommandType = System.Data.CommandType.StoredProcedure;
            readCommand.CommandText = $"[{QueueClient.QueueName}].[Read]";
            readCommand.Parameters.AddWithValue("subscriptionID", subscriptionID);
            readCommand.Parameters.Add("num", System.Data.SqlDbType.Int);
            readCommand.Parameters.Add("checkLockSeconds", System.Data.SqlDbType.Int);
            readCommand.Parameters.Add("peek", System.Data.SqlDbType.Bit);
            readCommand.Parameters.Add(new SqlParameter("newLockToken", System.Data.SqlDbType.UniqueIdentifier) {
                Direction = System.Data.ParameterDirection.Output });
            readCommand.Prepare();

            completeCommand = connection.CreateCommand();
            completeCommand.CommandType = System.Data.CommandType.StoredProcedure;
            completeCommand.CommandText = $"[{QueueClient.QueueName}].[Complete]";
            completeCommand.Parameters.AddWithValue("subscriptionID", subscriptionID);
            completeCommand.Parameters.Add("id", System.Data.SqlDbType.BigInt);
            completeCommand.Parameters.Add("currentLockToken", System.Data.SqlDbType.UniqueIdentifier);
            completeCommand.Prepare();

            relockCommand = connection.CreateCommand();
            relockCommand.CommandType = System.Data.CommandType.StoredProcedure;
            relockCommand.CommandText = $"[{QueueClient.QueueName}].[Relock]";
            relockCommand.Parameters.AddWithValue("subscriptionID", subscriptionID);
            relockCommand.Parameters.Add("currentLockToken", System.Data.SqlDbType.UniqueIdentifier);
            relockCommand.Prepare();

            unlockCommand = connection.CreateCommand();
            unlockCommand.CommandType = System.Data.CommandType.StoredProcedure;
            unlockCommand.CommandText = $"[{QueueClient.QueueName}].[Unlock]";
            unlockCommand.Parameters.AddWithValue("subscriptionID", subscriptionID);
            unlockCommand.Parameters.Add("currentLockToken", System.Data.SqlDbType.UniqueIdentifier);
            unlockCommand.Prepare();

            needReconnect = false;
        }

        private Message[] ReadInternal(int num = 1, int checkLockSeconds = -1, bool peek = false)
        {
            int realLockTimeout = checkLockSeconds < 0 ? defaultCheckLockSeconds : checkLockSeconds;

            int i = 0;
            Message[] messages = null;
            List<Message> list = null;

            if (num > 0)
            {
                messages = new Message[num];
            }
            else
            {
                list = new List<Message>();
            }

            Guid lockGuid = Guid.Empty;

            lock (lockObj)
            {
                if (currentLockToken != Guid.Empty)
                {
                    throw new Exception("Cannot start read while previous data is not completed");
                }

                if (needReconnect)
                {
                    Reconnect();
                }

                try
                {
                    readCommand.Parameters[1].Value = num;
                    readCommand.Parameters[2].Value = realLockTimeout;
                    readCommand.Parameters[3].Value = peek;

                    using (var r = readCommand.ExecuteReader())
                    {
                        if (num > 0)
                        {
                            while (r.Read())
                            {
                                messages[i++] = new Message(r.GetInt64(0), r.GetDateTime(1), r.GetSqlBytes(2).Value);
                            }
                        }
                        else
                        {
                            while (r.Read())
                            {
                                list.Add(new Message(r.GetInt64(0), r.GetDateTime(1), r.GetSqlBytes(2).Value));
                            }

                            messages = list.ToArray();
                            i = messages.Length;
                        }
                    }

                    if (readCommand.Parameters[4].Value != null && readCommand.Parameters[4].Value != DBNull.Value)
                    {
                        lockGuid = (Guid)readCommand.Parameters[4].Value;
                    }
                }
                catch
                {
                    needReconnect = true;
                    throw;
                }

                if (!peek)
                {
                    currentLockToken = lockGuid;
                    if (i > 0)
                    {
                        idToComplete = messages[i - 1].ID;
                    }
                }
            }

            Message[] res;
            if (i != messages.Length)
            {
                res = new Message[i];
                Array.Copy(messages, 0, res, 0, i);
            }
            else
            {
                res = messages;
            }

            return res;
        }

        public Message[] Read(int num = 1, int checkLockSeconds = -1)
        {
            return ReadInternal(num, checkLockSeconds, false);
        }

        public Message ReadOne(int checkLockSeconds = -1)
        {
            var messages = ReadInternal(1, checkLockSeconds, false);

            if (messages.Length > 0)
            {
                return messages[0];
            }
            else
            {
                return null;
            }
        }

        public Message[] ReadAll(int checkLockSeconds = -1)
        {
            return ReadInternal(-1, checkLockSeconds, false);
        }

        public Message[] Peek(int num = 1)
        {
            return ReadInternal(num, -1, true);
        }

        public Message PeekOne()
        {
            var messages = ReadInternal(1, -1, true);

            if (messages.Length > 0)
            {
                return messages[0];
            }
            else
            {
                return null;
            }
        }

        public Message[] PeekAll()
        {
            return ReadInternal(-1, -1, true);
        }

        public void Relock()
        {
            lock (lockObj)
            {
                if (currentLockToken == Guid.Empty)
                {
                    return;
                }

                if (needReconnect)
                {
                    Reconnect();
                }

                try
                {
                    relockCommand.Parameters[1].Value = currentLockToken;
                    relockCommand.ExecuteNonQuery();
                }
                catch
                {
                    needReconnect = true;
                    throw;
                }
            }
        }

        public void Complete()
        {
            lock (lockObj)
            {
                if (currentLockToken == Guid.Empty)
                {
                    return;
                }

                if (needReconnect)
                {
                    Reconnect();
                }

                try
                {
                    completeCommand.Parameters[1].Value = idToComplete;
                    completeCommand.Parameters[2].Value = currentLockToken;
                    completeCommand.ExecuteNonQuery();
                }
                catch
                {
                    needReconnect = true;
                    throw;
                }

                idToComplete = 0;
                currentLockToken = Guid.Empty;
            }
        }

        public void Unlock()
        {
            lock (lockObj)
            {
                if (currentLockToken == Guid.Empty)
                {
                    return;
                }

                if (needReconnect)
                {
                    Reconnect();
                }

                try
                {
                    unlockCommand.Parameters[1].Value = currentLockToken;
                    unlockCommand.ExecuteNonQuery();
                }
                catch
                {
                    needReconnect = true;
                    throw;
                }

                idToComplete = 0;
                currentLockToken = Guid.Empty;
            }
        }
    }
}
