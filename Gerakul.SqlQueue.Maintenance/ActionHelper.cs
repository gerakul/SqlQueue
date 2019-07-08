using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Maintenance
{
    public static class ActionHelper
    {
        private static readonly Regex regex = new Regex(@"^--(?<name>\w+)(=(?<value>.+))?$", 
            RegexOptions.IgnoreCase);

        private static KeyValuePair<string, string> ParseOption(string option)
        {
            var match = regex.Match(option);
            var optionName = (match.Groups["name"].Value ?? "").ToLowerInvariant();
            var optionValue = match.Groups["value"].Value;

            return new KeyValuePair<string, string>(optionName, optionValue);
        }

        public static OptionHelper ParseOptions(string[] args, int startIndex)
        {
            Dictionary<string, string> options = new Dictionary<string, string>();
            for (int i = startIndex; i < args.Length; i++)
            {
                var keyVal = ParseOption(args[i]);
                if (string.IsNullOrWhiteSpace(keyVal.Key))
                {
                    continue;
                }

                if (!options.ContainsKey(keyVal.Key))
                {
                    options.Add(keyVal.Key, keyVal.Value);
                }
            }

            return new OptionHelper(options);
        }
    }
}
