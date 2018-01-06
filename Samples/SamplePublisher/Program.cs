using System;
using System.IO;
using Adaptive.Aeron;
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
            [ProtoMember(1)]
            public int EventId { get; set; }
            [ProtoMember(2)]
            public string Bleb { get; set; }
        }

        static byte[] Serialize<T>(T value)
        {
            using (var ms = new MemoryStream())
            {
                ProtoBuf.Serializer.Serialize(ms, value);
                return ms.ToArray();
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
            var channel = "aeron:udp?endpoint=localhost:40123|interface=192.168.1.0/24";
            var maxMsgLength = OneMb;

            var context = new Adaptive.Aeron.Aeron.Context();

            IIdleStrategy offerIdleStrategy = new BusySpinIdleStrategy();

            // Connect to media driver and add publication to send messages on the configured channel and stream ID.
            // The Aeron and Publication classes implement AutoCloseable, and will automatically
            // clean up resources when this try block is finished.
            using (var aeron = Adaptive.Aeron.Aeron.Connect(context))
            using (var publication = aeron.AddPublication(channel, testStreamId))
            using (var byteBuffer = BufferUtil.AllocateDirectAligned(maxMsgLength, BitUtil.CACHE_LINE_LENGTH))
            using (var buffer = new UnsafeBuffer(byteBuffer))
            {
                do
                {
                    TestMessage tm = new TestMessage() { EventId = 123, Bleb = Guid.NewGuid().ToString() };
                    //Console.WriteLine($"Streaming {NumberOfMessages} messages of {(RandomMessageLength ? " random" : "")} size {MessageLength} bytes to {Channel} on stream Id {StreamID}");



                    Publish(tm, publication, offerIdleStrategy, buffer);

                    //Console.WriteLine("Done streaming. Back pressure ratio " + (double)backPressureCount / NumberOfMessages);

                    //if (0 < LingerTimeoutMs)
                    //{
                    //    Console.WriteLine("Lingering for " + LingerTimeoutMs + " milliseconds...");
                    //    Thread.Sleep((int)LingerTimeoutMs);
                    //}

                    //_printingActive = false;

                    Console.WriteLine("Execute again?");
                } while (Console.ReadLine() == "y");
            }
        }

        private static void Publish<T>(T message,
            Publication publication, IIdleStrategy offerIdleStrategy,
            UnsafeBuffer buffer)
        {
            var serTm = Serialize(message);
            var length = serTm.Length;

            buffer.PutBytes(0, serTm);

            offerIdleStrategy.Reset();
            while (publication.Offer(buffer, 0, length) < 0L)
            {
                // The offer failed, which is usually due to the publication
                // being temporarily blocked.  Retry the offer after a short
                // spin/yield/sleep, depending on the chosen IdleStrategy.
                //backPressureCount++;
                offerIdleStrategy.Idle();
            }

            //reporter.OnMessage(1, length);
        }
    }
}
