using System;
using System.IO;
using System.Text;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using ProtoBuf;

namespace SamplePublisher
{
    public class Program
    {
        [ProtoContract]
        public class TestMessage
        {
            [ProtoMember(1)] public int EventId { get; set; }
            [ProtoMember(2)] public string Bleb { get; set; }

            public override string ToString() => $"{nameof(EventId)}: {EventId}, {nameof(Bleb)}: {Bleb}";
        }

        static byte[] Serialize<T>(T value)
        {
            using (var ms = new MemoryStream())
            {
                ProtoBuf.Serializer.Serialize(ms, value);
                return ms.ToArray();
            }
        }

        static T Deserialize<T>(byte[] data)
        {
            using (var ms = new MemoryStream(data))
            {
                return ProtoBuf.Serializer.Deserialize<T>(ms);
            }
        }

        public static void Main()
        {
            var p = new Program();
            p.Run();
        }

        public void Run()
        {
            const int OneKb = 1024;
            const int OneMb = OneKb * 1024;

            var testStreamId = 99;
            var channel = "aeron:ipc";
            var maxMsgLength = OneMb;

            var context = new Aeron.Context();

            // Maximum number of message fragments to receive during a single 'poll' operation
            const int fragmentLimitCount = 10;

            Console.WriteLine("Subscribing to " + channel + " on stream Id " + testStreamId);

            var running = new AtomicBoolean(true);
            // Register a SIGINT handler for graceful shutdown.
            Console.CancelKeyPress += (s, e) => running.Set(false);

            // dataHandler method is called for every new datagram received
            void FragmentHandler(UnsafeBuffer buffer, int offset, int length, Header header)
            {
                var data = new byte[length];
                buffer.GetBytes(offset, data);

                var d = Deserialize<TestMessage>(data);
                Console.WriteLine(
                    $"Received message ({d}) to stream {testStreamId:D} from session {header.SessionId:x} term id {header.TermId:x} term offset {header.TermOffset:D} ({length:D}@{offset:D})");

                // Received the intended message, time to exit the program
                //running.Set(false);
            }

            var fragmentReassembler = new FragmentAssembler(FragmentHandler);

            // Create a context, needed for client connection to media driver
            // A separate media driver process need to run prior to running this application

            // Create an Aeron instance with client-provided context configuration, connect to the
            // media driver, and add a subscription for the given channel and stream using the supplied
            // dataHandler method, which will be called with new messages as they are received.
            // The Aeron and Subscription classes implement AutoCloseable, and will automatically
            // clean up resources when this try block is finished.
            using (var aeron = Aeron.Connect(context))
            using (var subscription = aeron.AddSubscription(channel, testStreamId))
            {
                IIdleStrategy idleStrategy = new BusySpinIdleStrategy();

                // Try to read the data from subscriber
                while (running.Get())
                {
                    // poll delivers messages to the dataHandler as they arrive
                    // and returns number of fragments read, or 0
                    // if no data is available.
                    var fragmentsRead = subscription.Poll(fragmentReassembler.Delegate(), fragmentLimitCount);
                    // Give the IdleStrategy a chance to spin/yield/sleep to reduce CPU
                    // use if no messages were received.
                    idleStrategy.Idle(fragmentsRead);
                }

                Console.WriteLine("Press any key...");
                Console.ReadLine();
            }
        }
    }
}