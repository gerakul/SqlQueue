using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Maintenance
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args == null || args.Length == 0)
            {
                Console.WriteLine("Cannot find any arguments");
                return;
            }

            var actionName = args[0].ToLowerInvariant();
            IAction action;

            switch (actionName)
            {
                case Actions.Export:
                    action = new ExportAction();
                    break;
                case Actions.Import:
                    action = new ImportAction();
                    break;
                default:
                    Console.WriteLine($"Action {actionName} is not supported");
                    return;
            }

            Console.WriteLine("Start action...");

            action.Execute(args).Wait();

            Console.WriteLine("Complete");
        }
    }
}
