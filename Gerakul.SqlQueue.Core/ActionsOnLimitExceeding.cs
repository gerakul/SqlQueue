using System;
using System.Collections.Generic;
using System.Text;

namespace Gerakul.SqlQueue.Core
{
    public enum ActionsOnLimitExceeding
    {
        DeleteSubscription = 1,
        DisableSubscription = 2
    }
}
