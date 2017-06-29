using System;
using System.Collections.Generic;
using System.Text;

namespace Gerakul.SqlQueue.Core
{
    public interface IWriter
    {
        long Write(byte[] data);
    }
}
