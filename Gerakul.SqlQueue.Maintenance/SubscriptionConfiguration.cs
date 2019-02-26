using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Maintenance
{
    public class SubscriptionConfiguration
    {
        public string Name { get; set; }
        public bool Disabled { get; set; }
        public int? MaxIdleIntervalSeconds { get; set; }
        public int? MaxUncompletedMessages { get; set; }
        public int? ActionOnLimitExceeding { get; set; }
    }
}
