package protocols.membership.hyparview.messages;

import java.io.IOException;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

public class ForwardJoinReply extends ProtoMessage {
	public final static short MSG_ID = 6969;

    private final Host newNode;

    public ForwardJoinReply(Host newNode) {
        super(MSG_ID);
        this.newNode = newNode;
    }

    public Host getnewNode() {
        return newNode;
    }
    
    @Override
    public String toString() {
        return "New Node = " + newNode.toString();
    }
    
    public static ISerializer<ForwardJoinReply> serializer = new ISerializer<ForwardJoinReply>() {
        @Override
        public void serialize(ForwardJoinReply joinRequest, ByteBuf out) throws IOException {
                Host.serializer.serialize(joinRequest.getnewNode(), out);
        }

        @Override
        public ForwardJoinReply deserialize(ByteBuf in) throws IOException {
        	Host node = Host.serializer.deserialize(in);
            return new ForwardJoinReply(node);
        }
    };

}
