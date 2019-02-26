using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Maintenance
{
    public class QueueConfiguration
    {
        public string Name { get; set; }
        public int MinNum { get; set; }
        public int TresholdNum { get; set; }

        public List<SubscriptionConfiguration> Subscriptions { get; set; } = new List<SubscriptionConfiguration>();
    }
}
