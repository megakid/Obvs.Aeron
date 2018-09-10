using System;
using System.IO;
using System.Linq;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Sample.Shared;

namespace Sample.Publisher
{
    public class Program
    {
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

            var context = new Adaptive.Aeron.Aeron.Context();

            IIdleStrategy offerIdleStrategy = new SpinWaitIdleStrategy();

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
                    TestMessage tm = new TestMessage() { EventId = 123, Bleb = string.Join(",", Enumerable.Repeat(Guid.NewGuid().ToString(), 100)) };
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
            Publication publication, 
            IIdleStrategy offerIdleStrategy,
            IMutableDirectBuffer buffer)
        {
            var serTm = Util.Serialize(message);
            var length = serTm.Length;

            buffer.PutBytes(0, serTm);

            offerIdleStrategy.Reset();
            while (!Offer(publication, buffer, length))
            {
                // The offer failed, which is usually due to the publication
                // being temporarily blocked.  Retry the offer after a short
                // spin/yield/sleep, depending on the chosen IdleStrategy.
                //backPressureCount++;
                offerIdleStrategy.Idle();
            }

            //reporter.OnMessage(1, length);
        }

        private static bool Offer(Publication publication, IDirectBuffer buffer, int length)
        {
            var result = publication.Offer(buffer, 0, length);

            if (result >= 0L)
                return true;

            switch (result)
            {
                case Publication.BACK_PRESSURED:
                    Console.WriteLine(" Offer failed due to back pressure");
                    return false;
                case Publication.ADMIN_ACTION:
                    Console.WriteLine("Offer failed because of an administration action in the system");
                    return false;
                case Publication.NOT_CONNECTED:
                    Console.WriteLine(" Offer failed because publisher is not connected to subscriber");
                    return true;
                case Publication.CLOSED:
                    Console.WriteLine("Offer failed publication is closed");
                    return true;
            }

            Console.WriteLine(" Offer failed due to unknown reason");
            return false;
        }
    }
}
