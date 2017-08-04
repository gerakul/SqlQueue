using System;
using System.Collections.Generic;
using System.Text;

namespace Gerakul.SqlQueue.InMemory
{
    public sealed class AutoReaderOptions
    {
        public int MinDelayMilliseconds { get; set; } = 100;
        public int MaxDelayMilliseconds { get; set; } = 5000;
        public int NumPerReed { get; set; } = -1;
        public bool UnlockIfExceptionWasThrownByHandling { get; set; } = true;
    }
}
