using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Sample.Shared;

namespace Sample.Subscriber
{
    public class Program
    {

        public static void Main()
        {
            using (AeronThreadManager atm = new AeronThreadManager())
            {
                atm.Start();

                Console.WriteLine("Press any key to exit...");
                
                Console.ReadKey();
            }
        }
    }

    class AeronThreadManager : IDisposable
    {
        readonly AtomicBoolean _started = new AtomicBoolean(false);
        readonly AtomicBoolean _stopping = new AtomicBoolean(false);
        private readonly Thread _thread;

        public AeronThreadManager()
        {
            _thread = new Thread(Run) { Name = "ObvsAeronSubscriberThread", IsBackground = true };
        }

        public void Start()
        {
            if (!_started.CompareAndSet(false, true))
                return;
            
            _thread.Start();
        }

        private void Run()
        {
            const int OneKb = 1024;
            const int OneMb = OneKb * 1024;

            var testStreamId = 99;
            var channel = "aeron:ipc";
            var maxMsgLength = OneMb;

            // Maximum number of message fragments to receive during a single 'poll' operation
            const int fragmentLimitCount = 10;

            Console.WriteLine("Subscribing to " + channel + " on stream Id " + testStreamId);



            // Create a context, needed for client connection to media driver
            // A separate media driver process need to run prior to running this application

            // Create an Aeron instance with client-provided context configuration, connect to the
            // media driver, and add a subscription for the given channel and stream using the supplied
            // dataHandler method, which will be called with new messages as they are received.
            // The Aeron and Subscription classes implement AutoCloseable, and will automatically
            // clean up resources when this try block is finished.
            using (var aeron = Aeron.Connect(new Aeron.Context()))
            using (var subscription = aeron.AddSubscription(channel, testStreamId))
            {
                IIdleStrategy idleStrategy = new SpinWaitIdleStrategy();

                // Try to read the data from subscriber
                while (_stopping.Get())
                {
                    // poll delivers messages to the dataHandler as they arrive
                    // and returns number of fragments read, or 0
                    // if no data is available.
                    var fragmentsRead = subscription.Poll(new FragmentAssembler((buffer, offset, length, header) => {}), fragmentLimitCount);
                    // Give the IdleStrategy a chance to spin/yield/sleep to reduce CPU
                    // use if no messages were received.
                    idleStrategy.Idle(fragmentsRead);
                }
            }
        }
        
        //private SubscriptionCollection _subscriptions = new SubscriptionCollection();

        public void Dispose()
        {
            _stopping.Set(true);
            _thread.Join(TimeSpan.FromSeconds(5));
        }
    }

    class SubscriptionCollection : IDisposable
    {
        private readonly Aeron _aeron;
        private List<AeronSubscription> _subscriptions;
        
        public SubscriptionCollection(Aeron aeron)
        {
            _aeron = aeron;
        }

        public void Subscribe(string channel, int streamId)
        {
            var subs = new List<AeronSubscription>(_subscriptions ?? new List<AeronSubscription>())
            {
                new AeronSubscription(_aeron.AddSubscription(channel, streamId))
            };
            
            _subscriptions = subs;
        }
        
        public int Collect()
        {
            var subscriptions = _subscriptions;
            
            return subscriptions.Sum(s => CollectSubscription(s));
        }

        private static int CollectSubscription(AeronSubscription s)
        {
            try
            {
                return s.Collect();
            }
            catch
            {
                // ignored
                return 0;
            }
        }

        public void Dispose()
        {
            var subscriptions = _subscriptions;
            
            _subscriptions = null;
            
            foreach (var subscription in subscriptions)
            {
                subscription.Dispose();
            }
            
            _aeron?.Dispose();
        }
    }
    internal class AeronSubscription : IDisposable
    {
        private const int FragmentLimit = 10;
        private readonly Subscription _subscription;
        private readonly FragmentAssembler _fragmentReassembler;
        private readonly ISubject<byte[]> _subject = Subject.Synchronize(new Subject<byte[]>());

        public AeronSubscription(Subscription subscription)
        {
            _subscription = subscription;
            
            
            // dataHandler method is called for every new datagram received

            _fragmentReassembler = new FragmentAssembler(HandlerHelper.ToFragmentHandler(CompleteMessageReceived));
        }

        public IObservable<byte[]> Messages => _subject; 

        private void CompleteMessageReceived(IDirectBuffer buffer, int offset, int length, Header header)
        {
            var data = new byte[length];
            buffer.GetBytes(offset, data);
//
//            var d = Util.Deserialize<TestMessage>(data);
//            Console.WriteLine($"Received message ({d}) to stream {testStreamId:D} from session {header.SessionId:x} term id {header.TermId:x} term offset {header.TermOffset:D} ({length:D}@{offset:D})");

            _subject.OnNext(data);
            // Received the intended message, time to exit the program
            //running.Set(false);
        }

        public AeronSubscription(string channel, int streamId, Aeron aeron)
            : this(aeron.AddSubscription(channel, streamId))
        {
        }
        
        public void Dispose()
        {
            _subscription?.Dispose();
        }

        public int Collect()
        {
            return _subscription.Poll(_fragmentReassembler, FragmentLimit);
        }
    }
}