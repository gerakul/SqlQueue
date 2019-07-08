using Gerakul.SqlQueue.Maintenance.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Maintenance
{
    public class OptionHelper
    {
        private Dictionary<string, string> optionDictionary;

        public OptionHelper(Dictionary<string, string> optionDictionary)
        {
            this.optionDictionary = optionDictionary.ToDictionary(x => x.Key.ToLowerInvariant(), x => x.Value);
        }

        public string GetString(string optionName)
        {
            if (optionDictionary.TryGetValue(optionName.ToLowerInvariant(), out var value))
            {
                return value;
            }

            throw new OptionNotFoundException(optionName);
        }

        public bool TryGetString(string optionName, out string value)
        {
            return optionDictionary.TryGetValue(optionName.ToLowerInvariant(), out value);
        }

        public bool GetBool(string optionName)
        {
            if (optionDictionary.TryGetValue(optionName.ToLowerInvariant(), out var optVal))
            {
                if (bool.TryParse(optVal, out bool boolVal))
                {
                    return boolVal;
                }
                else if (int.TryParse(optVal, out int intVal) && (intVal == 0 || intVal == 1))
                {
                    return intVal != 0;
                }

                throw new OptionHasIncorrectFormatException(optionName);
            }

            throw new OptionNotFoundException(optionName);
        }

        public bool TryGetBool(string optionName, out bool value)
        {
            if (optionDictionary.TryGetValue(optionName.ToLowerInvariant(), out var optVal))
            {
                if (bool.TryParse(optVal, out bool boolVal))
                {
                    value = boolVal;
                    return true;
                }
                else if (int.TryParse(optVal, out int intVal) && (intVal == 0 || intVal == 1))
                {
                    value = intVal != 0;
                    return true;
                }

                throw new OptionHasIncorrectFormatException(optionName);
            }

            value = default;
            return false;
        }

        public bool IsExists(string optionName)
        {
            return optionDictionary.ContainsKey(optionName.ToLowerInvariant());
        }
    }
}
