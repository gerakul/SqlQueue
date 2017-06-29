using System;
using System.Collections.Generic;
using System.Text;

namespace Gerakul.SqlQueue.Core
{
    public interface IReader
    {
        Message[] Read(int num = 1, int checkLockSeconds = -1);
        Message ReadOne(int checkLockSeconds = -1);
        Message[] ReadAll(int checkLockSeconds = -1);
        Message[] Peek(int num = 1);
        Message PeekOne();
        Message[] PeekAll();
        void Relock();
        void Complete();
        void Unlock();
    }
}
