using System;
using System.Collections.Generic;
using System.Text;

namespace Gerakul.SqlQueue.Core
{
    public class SubscriptionInfo
    {
        public int ID { get; }
        public string Name { get; }
        public long LastCompletedID { get; }
        public DateTime LastCompletedTime { get; }
        public DateTime? LockTime { get; }
        public bool Disabled { get; }

        public long UncompletedMessages { get; }
        public long IdleIntervalSeconds { get; }

        public SubscriptionSettings Settings { get; }

        public SubscriptionInfo(int id, string name, long lastCompletedID, DateTime lastCompletedTime, DateTime? lockTime, 
            bool disabled, long uncompletedMessages, long idleIntervalSeconds, SubscriptionSettings settings)
        {
            ID = id;
            Name = name;
            LastCompletedID = lastCompletedID;
            LastCompletedTime = lastCompletedTime;
            LockTime = lockTime;
            Disabled = disabled;
            UncompletedMessages = uncompletedMessages;
            IdleIntervalSeconds = idleIntervalSeconds;
            Settings = settings;
        }
    }
}
