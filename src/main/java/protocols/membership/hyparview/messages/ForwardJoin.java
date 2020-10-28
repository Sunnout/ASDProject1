package protocols.membership.hyparview.messages;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;
import protocols.membership.full.messages.SampleMessage;

public class ForwardJoin extends ProtoMessage {

    public final static short MSG_ID = 112;

    private final Set<Host> newNode;

    public ForwardJoin(Set<Host> sample) {
        super(MSG_ID);
        this.sample = sample;
    }

    public Set<Host> getSample() {
        return sample;
    }

    @Override
    public String toString() {
        return "ShuffleMessage{" +
                "subset=" + sample +
                '}';
    }

    public static ISerializer<SampleMessage> serializer = new ISerializer<SampleMessage>() {
        @Override
        public void serialize(SampleMessage sampleMessage, ByteBuf out) throws IOException {
            out.writeInt(sampleMessage.sample.size());
            for (Host h : sampleMessage.sample)
                Host.serializer.serialize(h, out);
        }

        @Override
        public SampleMessage deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            Set<Host> subset = new HashSet<>(size, 1);
            for (int i = 0; i < size; i++)
                subset.add(Host.serializer.deserialize(in));
            return new SampleMessage(subset);
        }
    };
}
