using Gerakul.SqlQueue.InMemory.Core;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

namespace Gerakul.SqlQueue.InMemory
{
    public class QueueClient : ISubscriptionManager
    {
        public string ConnectionString { get; }
        public string QueueName { get; }

        internal QueueClient(string connectionString, string name)
        {
            this.ConnectionString = connectionString;
            this.QueueName = name;
        }

        public static QueueClient Create(string connectionString, string name)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var cmd = conn.CreateCommand();
                cmd.CommandText = "select top 1 1 from [INFORMATION_SCHEMA].[SCHEMATA] where [SCHEMA_NAME] = @name";
                cmd.Parameters.AddWithValue("name", name);

                var x = cmd.ExecuteScalar();

                if (x == null || x == DBNull.Value || (int)x != 1)
                {
                    throw new Exception($"Queue '{name}' is not found");
                }
            }

            return new QueueClient(connectionString, name);
        }

        public int CreateSubscription(string name)
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();

                var cmd = conn.CreateCommand();
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.CommandText = $"[{QueueName}].[CreateSubscription]";
                cmd.Parameters.AddWithValue("name", name);
                cmd.Parameters.Add(new SqlParameter("subscriptionID", System.Data.SqlDbType.Int) { Direction = System.Data.ParameterDirection.Output });

                cmd.ExecuteNonQuery();

                return (int)cmd.Parameters[1].Value;
            }
        }

        public void DeleteSubscription(string name)
        {
            var subID = FindSubscriptionOrThrowException(name);

            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();

                var cmd = conn.CreateCommand();
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.CommandText = $"[{QueueName}].[DeleteSubscription]";
                cmd.Parameters.AddWithValue("subscriptionID", subID);

                cmd.ExecuteNonQuery();
            }
        }

        public void EnableSubscription(string name)
        {
            var subID = FindSubscriptionOrThrowException(name);

            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();

                var cmd = conn.CreateCommand();
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.CommandText = $"[{QueueName}].[EnableSubscription]";
                cmd.Parameters.AddWithValue("subscriptionID", subID);

                cmd.ExecuteNonQuery();
            }
        }

        public void DisableSubscription(string name)
        {
            var subID = FindSubscriptionOrThrowException(name);

            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();

                var cmd = conn.CreateCommand();
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.CommandText = $"[{QueueName}].[DisableSubscription]";
                cmd.Parameters.AddWithValue("subscriptionID", subID);

                cmd.ExecuteNonQuery();
            }
        }

        public int FindSubscription(string name)
        {
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                conn.Open();

                var cmd = conn.CreateCommand();
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.CommandText = $"[{QueueName}].[FindSubscription]";
                cmd.Parameters.AddWithValue("name", name);

                using (var r = cmd.ExecuteReader())
                {
                    if (r.Read())
                    {
                        return r.GetInt32(0);
                    }
                    else
                    {
                        return 0;
                    }
                }
            }
        }

        internal int FindSubscriptionOrThrowException(string name)
        {
            var subID = FindSubscription(name);

            if (subID == 0)
            {
                throw new Exception($"Subscription '{name}' is not found");
            }

            return subID;
        }

        public Writer CreateWriter(int cleanMinIntervalSeconds = 10)
        {
            return new Writer(this, cleanMinIntervalSeconds);
        }

        public Reader CreateReader(string subscription, int defaultCheckLockSeconds = 30)
        {
            FindSubscriptionOrThrowException(subscription);
            return new Reader(this, subscription, defaultCheckLockSeconds);
        }

        public AutoReader CreateAutoReader(string subscription)
        {
            FindSubscriptionOrThrowException(subscription);
            return new AutoReader(this, subscription);
        }
    }
}
