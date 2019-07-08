using Gerakul.SqlQueue.Maintenance.Actions;
using Gerakul.SqlQueue.Maintenance.Exceptions;
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
            var output = new ConsoleOutput();

            if (args == null || args.Length == 0)
            {
                output.WriteLine("Cannot find any arguments");
                return;
            }

            var actionName = args[0].ToLowerInvariant();
            IAction action;

            switch (actionName)
            {
                case ActionList.Export:
                    action = new ExportAction();
                    break;
                case ActionList.Import:
                    action = new ImportAction(output);
                    break;
                case ActionList.FullReset:
                    action = new FullResetAction();
                    break;
                case ActionList.ForceClean:
                    action = new ForceCleanAction();
                    break;
                default:
                    output.WriteLine($"Action {actionName} is not supported");
                    return;
            }

            output.WriteLine("Start action...");

            try
            {
                action.Execute(args).GetAwaiter().GetResult();
            }
            catch (OptionException ex)
            {
                output.WriteLine(ex.Message);
                return;
            }

            output.WriteLine("Complete");
        }
    }
}
