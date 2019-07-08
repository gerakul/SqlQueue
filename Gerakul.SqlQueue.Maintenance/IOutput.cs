using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Maintenance
{
    public interface IOutput
    {
        Task Write(string value);
        Task WriteLine(string value);
    }
}
