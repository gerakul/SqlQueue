﻿using Gerakul.SqlQueue.Core;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.SqlTypes;

namespace Gerakul.SqlQueue.InMemory
{
    public sealed class QueueClient : ISubscriptionManager
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

        public int CreateSubscription(string name, SubscriptionSettings settings = null)
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();

                var cmd = conn.CreateCommand();
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.CommandText = $"[{QueueName}].[CreateSubscription]";
                cmd.Parameters.AddWithValue("name", name);

                cmd.Parameters.AddWithValue("maxIdleIntervalSeconds", settings?.MaxIdleIntervalSeconds != null
                    ? new SqlInt32(settings.MaxIdleIntervalSeconds.Value) : SqlInt32.Null);
                cmd.Parameters.AddWithValue("maxUncompletedMessages", settings?.MaxUncompletedMessages != null
                    ? new SqlInt32(settings.MaxUncompletedMessages.Value) : SqlInt32.Null);
                cmd.Parameters.AddWithValue("actionOnLimitExceeding", settings?.ActionOnLimitExceeding != null
                    ? new SqlInt32((int)settings.ActionOnLimitExceeding.Value) : SqlInt32.Null);

                cmd.Parameters.Add(new SqlParameter("subscriptionID", System.Data.SqlDbType.Int) { Direction = System.Data.ParameterDirection.Output });

                cmd.ExecuteNonQuery();

                return (int)cmd.Parameters[4].Value;
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

        public void UpdateSubscription(string name, SubscriptionSettings settings)
        {
            var subID = FindSubscriptionOrThrowException(name);

            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();

                var cmd = conn.CreateCommand();
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.CommandText = $"[{QueueName}].[SetSubscriptionSettings]";
                cmd.Parameters.AddWithValue("subscriptionID", subID);
                cmd.Parameters.AddWithValue("maxIdleIntervalSeconds", settings.MaxIdleIntervalSeconds.HasValue 
                    ? new SqlInt32(settings.MaxIdleIntervalSeconds.Value) : SqlInt32.Null);
                cmd.Parameters.AddWithValue("maxUncompletedMessages", settings.MaxUncompletedMessages.HasValue
                    ? new SqlInt32(settings.MaxUncompletedMessages.Value) : SqlInt32.Null);
                cmd.Parameters.AddWithValue("actionOnLimitExceeding", settings.ActionOnLimitExceeding.HasValue
                    ? new SqlInt32((int)settings.ActionOnLimitExceeding.Value) : SqlInt32.Null);

                cmd.ExecuteNonQuery();
            }
        }

        public SubscriptionInfo GetSubscriptionInfo(string name)
        {
            var subID = FindSubscriptionOrThrowException(name);

            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();

                var cmd = conn.CreateCommand();
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.CommandText = $"[{QueueName}].[GetSubscriptionInfo]";
                cmd.Parameters.AddWithValue("subscriptionID", subID);

                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();

                    return ReadSubscriptionInfo(reader);
                }
            }
        }

        public IEnumerable<SubscriptionInfo> GetAllSubscriptionsInfo()
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();

                var cmd = conn.CreateCommand();
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.CommandText = $"[{QueueName}].[GetAllSubscriptionsInfo]";

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        yield return ReadSubscriptionInfo(reader);
                    }
                }
            }
        }

        private SubscriptionInfo ReadSubscriptionInfo(SqlDataReader reader)
        {
            return new SubscriptionInfo(reader.GetInt32(0), reader.GetString(1), reader.GetInt64(2), reader.GetDateTime(3), 
                reader.IsDBNull(4) ? (DateTime?)null : reader.GetDateTime(4), reader.GetBoolean(5), 
                reader.GetInt64(9), reader.GetInt32(10), 
                new SubscriptionSettings()
                {
                    MaxIdleIntervalSeconds = reader.IsDBNull(6) ? (int?)null : reader.GetInt32(6),
                    MaxUncompletedMessages = reader.IsDBNull(7) ? (int?)null : reader.GetInt32(7),
                    ActionOnLimitExceeding = reader.IsDBNull(8) ? (ActionsOnLimitExceeding?)null : (ActionsOnLimitExceeding)reader.GetInt32(8)
                });
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

        public AutoReader CreateAutoReader(string subscription, AutoReaderOptions options = null)
        {
            FindSubscriptionOrThrowException(subscription);
            return new AutoReader(this, subscription, options);
        }
    }
}
