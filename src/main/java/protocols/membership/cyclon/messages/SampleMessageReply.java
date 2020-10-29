package protocols.membership.cyclon.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SampleMessageReply extends SampleMessage {

    public final static short MSG_ID = 1002;

    public SampleMessageReply(Set<Host> sample) {
        super(sample);
    }
}
