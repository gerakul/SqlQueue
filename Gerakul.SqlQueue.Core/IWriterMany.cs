using System;
using System.Collections.Generic;
using System.Text;

namespace Gerakul.SqlQueue.Core
{
    public interface IWriterMany
    {
        long[] WriteMany(IEnumerable<byte[]> data, bool returnIDs = false);
    }
}
