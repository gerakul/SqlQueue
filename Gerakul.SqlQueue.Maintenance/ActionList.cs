using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Maintenance
{
    public static class ActionList
    {
        public const string Export = "export";
        public const string Import = "import";
        public const string FullReset = "fullreset";
        public const string ForceClean = "forceclean";
    }
}
