using Gerakul.SqlQueue.Core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.InMemory
{
    public class AutoReader : IAutoReader
    {
        private Reader reader;
        private CancellationTokenSource receivingLoopCTS;
        private CancellationTokenSource relockingLoopCTS;
        private bool started = false;
        private bool stopping = false;
        private object lockObject = new object();
        private Task EndTask;
        private DateTime lastRelock = DateTime.MinValue;

        internal AutoReader(QueueClient queueClient, string subscription)
        {
            reader = new Reader(queueClient, subscription, 30);
        }

        public Task Start(Func<Message[], Task> handler, int minDelayMilliseconds, int maxDelayMilliseconds, int numPerReed = -1)
        {
            lock (lockObject)
            {
                if (started || stopping)
                {
                    return Task.CompletedTask;
                }

                started = true;
                EndTask = new Task(() => { });
            }

            relockingLoopCTS = new CancellationTokenSource();
            receivingLoopCTS = new CancellationTokenSource();

            Task.Factory.StartNew(() => RelockingLoop(relockingLoopCTS.Token), TaskCreationOptions.LongRunning).ConfigureAwait(false);
            Task.Factory.StartNew(() => ReceivingLoop(handler, minDelayMilliseconds, maxDelayMilliseconds, numPerReed, receivingLoopCTS.Token), TaskCreationOptions.LongRunning).ConfigureAwait(false);
            return Task.CompletedTask;
        }

        public async Task Stop()
        {
            lock (lockObject)
            {
                if (!started || stopping)
                {
                    return;
                }

                stopping = true;
            }

            receivingLoopCTS.Cancel();
            await Task.WhenAll(EndTask).ConfigureAwait(false);
            receivingLoopCTS = null;
            relockingLoopCTS = null;

            lock (lockObject)
            {
                started = false;
                stopping = false;
            }
        }

        private async Task ReceivingLoop(Func<Message[], Task> handler, int minDelayMilliseconds, int maxDelayMilliseconds, int numPerReed,
            CancellationToken cancellationToken)
        {
            int delay = minDelayMilliseconds;
            Stopwatch sw = new Stopwatch();
            while (!cancellationToken.IsCancellationRequested)
            {
                var messages = reader.Read(numPerReed);

                if (messages?.Length > 0)
                {
                    sw.Start();
                    await handler(messages).ConfigureAwait(false);
                    reader.Complete();
                    sw.Stop();

                    if (messages.Length > 1)
                    {
                        delay = delay / messages.Length;

                        if (delay < minDelayMilliseconds)
                        {
                            delay = minDelayMilliseconds;
                        }
                    }
                }
                else
                {
                    delay = delay * 2;

                    if (delay > maxDelayMilliseconds)
                    {
                        delay = maxDelayMilliseconds;
                    }
                }

                var actualDelay = delay - (int)sw.ElapsedMilliseconds;
                sw.Reset();

                if (actualDelay > 0)
                {
                    try
                    {
                        await Task.Delay(actualDelay, cancellationToken).ConfigureAwait(false);
                    }
                    catch (TaskCanceledException ex)
                    {
                    }
                }
            }

            relockingLoopCTS.Cancel();
        }

        private async Task RelockingLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var t = DateTime.UtcNow;
                if ((t - lastRelock).TotalSeconds > 5)
                {
                    reader.Relock();
                    lastRelock = t;
                }

                try
                {
                    await Task.Delay(1000, cancellationToken).ConfigureAwait(false);
                }
                catch (TaskCanceledException ex)
                {
                }
            }

            EndTask.Start();
        }
    }
}
