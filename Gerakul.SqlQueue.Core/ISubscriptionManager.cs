using System;
using System.Collections.Generic;
using System.Text;

namespace Gerakul.SqlQueue.InMemory.Core
{
    public interface ISubscriptionManager
    {
        int CreateSubscription(string name);
        void DeleteSubscription(string name);
        void EnableSubscription(string name);
        void DisableSubscription(string name);
        int FindSubscription(string name);
    }
}
