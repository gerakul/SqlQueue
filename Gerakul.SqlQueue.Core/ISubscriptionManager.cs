using System;
using System.Collections.Generic;
using System.Text;

namespace Gerakul.SqlQueue.Core
{
    public interface ISubscriptionManager
    {
        int CreateSubscription(string name, SubscriptionSettings settings = null);
        int FindSubscription(string name);
        void DeleteSubscription(string name);
        void EnableSubscription(string name);
        void DisableSubscription(string name);
        void UpdateSubscription(string name, SubscriptionSettings settings);

        SubscriptionInfo GetSubscriptionInfo(string name);
        IEnumerable<SubscriptionInfo> GetAllSubscriptionsInfo();
    }
}
