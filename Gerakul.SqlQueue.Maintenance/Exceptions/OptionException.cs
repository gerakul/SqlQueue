using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Maintenance.Exceptions
{
    public class OptionException : Exception
    {
        public string Option { get; }

        public OptionException(string option, string message) : base(message)
        {
            Option = option;
        }
    }
}
