using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Maintenance.Exceptions
{
    public class OptionNotFoundException : OptionException
    {
        public OptionNotFoundException(string option) : base(option, $"Option {option} is not found")
        {
        }
    }
}
