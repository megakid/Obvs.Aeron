using System;
using ProtoBuf;

namespace Sample.Shared
{
    [ProtoContract]
    public class TestMessage
    {
        [ProtoMember(1)] public int EventId { get; set; }
        [ProtoMember(2)] public string Bleb { get; set; }

        public override string ToString() => $"{nameof(EventId)}: {EventId}, {nameof(Bleb)}: {Bleb}";



    }
}
