﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.Core
{
    public interface IAutoReader
    {
        Task Start(Func<Message[], Task> handler);
        Task Stop();
    }
}
