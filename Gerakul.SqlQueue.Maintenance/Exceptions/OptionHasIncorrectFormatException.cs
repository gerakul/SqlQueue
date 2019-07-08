using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Maintenance.Exceptions
{
    public class OptionHasIncorrectFormatException : OptionException
    {
        public OptionHasIncorrectFormatException(string option) : base(option, $"Option {option} has incorrect format")
        {
        }
    }
}
