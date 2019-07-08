using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Maintenance
{
    public class ConsoleOutput : IOutput
    {
        public Task Write(string value)
        {
            Console.Write(value);
            return Task.CompletedTask;
        }

        public Task WriteLine(string value)
        {
            Console.WriteLine(value);
            return Task.CompletedTask;
        }
    }
}
