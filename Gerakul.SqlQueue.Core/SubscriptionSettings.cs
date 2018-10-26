using System;
using System.Collections.Generic;
using System.Text;

namespace Gerakul.SqlQueue.Core
{
    public class SubscriptionSettings
    {
        public int? MaxIdleIntervalSeconds { get; set; }
        public int? MaxUncompletedMessages { get; set; }
        public ActionsOnLimitExceeding? ActionOnLimitExceeding { get; set; }
    }
}
