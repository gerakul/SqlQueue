using Gerakul.SqlQueue.Core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Gerakul.SqlQueue.InMemory
{
    public sealed class AutoReader : IAutoReader, IDisposable
    {
        public QueueClient QueueClient { get; }
        public string Subscription { get; }

        private Reader reader;
        private AutoReaderOptions options;
        private CancellationTokenSource receivingLoopCTS;
        private CancellationTokenSource relockingLoopCTS;
        private bool started = false;
        private bool stopping = false;
        private object lockObject = new object();
        private Task EndTask;
        private DateTime lastRelock = DateTime.MinValue;

        public event EventHandler<ExceptionThrownEventArgs> ExceptionThrown;

        private void OnExceptionThrown(ExceptionThrownEventArgs eventArgs)
        {
            var handler = ExceptionThrown;
            handler?.Invoke(this, eventArgs);
        }

        internal AutoReader(QueueClient queueClient, string subscription, AutoReaderOptions options)
        {
            this.options = options ?? new AutoReaderOptions();
            this.QueueClient = queueClient;
            this.Subscription = subscription;
            reader = new Reader(queueClient, subscription, 30);
        }

        public Task Start(Func<Message[], Task> handler)
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
            Task.Factory.StartNew(() => ReceivingLoop(handler, receivingLoopCTS.Token), TaskCreationOptions.LongRunning).ConfigureAwait(false);
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

        private async Task ReceivingLoop(Func<Message[], Task> handler, CancellationToken cancellationToken)
        {
            int delay = options.MinDelayMilliseconds;
            Stopwatch sw = new Stopwatch();
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var messages = reader.Read(options.NumPerReed);

                    if (messages?.Length > 0)
                    {
                        sw.Start();

                        try
                        {
                            await handler(messages).ConfigureAwait(false);
                            reader.Complete();
                        }
                        catch
                        {
                            if (options.UnlockIfExceptionWasThrownByHandling)
                            {
                                reader.Unlock();
                            }
                            else
                            {
                                reader.Complete();
                            }
                        }

                        sw.Stop();

                        if (messages.Length > 1)
                        {
                            delay = delay / messages.Length;

                            if (delay < options.MinDelayMilliseconds)
                            {
                                delay = options.MinDelayMilliseconds;
                            }
                        }
                    }
                    else
                    {
                        delay = delay * 2;

                        if (delay > options.MaxDelayMilliseconds)
                        {
                            delay = options.MaxDelayMilliseconds;
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
                        catch (TaskCanceledException)
                        {
                        }
                    }
                }
                catch (Exception ex)
                {
                    var eventArgs = new ExceptionThrownEventArgs(ex, ExceptionSite.ReceivingLoop);
                    OnExceptionThrown(eventArgs);
                    if (eventArgs.Stop)
                    {
                        Task.Run(() => Stop());
                        break;
                    }

                    try
                    {
                        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    }
                    catch (TaskCanceledException)
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
                try
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
                    catch (TaskCanceledException)
                    {
                    }
                }
                catch (Exception ex)
                {
                    var eventArgs = new ExceptionThrownEventArgs(ex, ExceptionSite.RelockingLoop);
                    OnExceptionThrown(eventArgs);
                    if (eventArgs.Stop)
                    {
                        Task.Run(() => Stop());
                        break;
                    }

                    try
                    {
                        await Task.Delay(1000, cancellationToken).ConfigureAwait(false);
                    }
                    catch (TaskCanceledException)
                    {
                    }
                }
            }

            EndTask.Start();
        }

        public void Close()
        {
            reader?.Close();
            GC.SuppressFinalize(this);
        }

        void IDisposable.Dispose()
        {
            Close();
        }
    }

    public class ExceptionThrownEventArgs : EventArgs
    {
        public Exception Exception { get; private set; }
        public ExceptionSite Site { get; private set; }
        public bool Stop { get; set; } = false;

        public ExceptionThrownEventArgs(Exception exception, ExceptionSite site)
        {
            this.Exception = exception;
            this.Site = site;
        }
    }

    public enum ExceptionSite
    {
        ReceivingLoop = 1,
        RelockingLoop = 2
    }
}
