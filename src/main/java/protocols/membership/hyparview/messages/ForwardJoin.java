package protocols.membership.hyparview.messages;

import java.io.IOException;
import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

public class ForwardJoin extends ProtoMessage {

    public final static short MSG_ID = 111;

    private final Host newNode;
    private final int ttl;

    public ForwardJoin(Host newNode, int ttl) {  // TODO add TTL
        super(MSG_ID);
        this.newNode = newNode;
        this.ttl = ttl;
    }

    public Host getnewNode() {
        return newNode;
    }
    
    public int getTTL() {
    	return ttl;
    }

    @Override
    public String toString() {
        return "New Node = " + newNode.toString();
    }
    
    public static ISerializer<ForwardJoin> serializer = new ISerializer<ForwardJoin>() {
        @Override
        public void serialize(ForwardJoin joinRequest, ByteBuf out) throws IOException {
                Host.serializer.serialize(joinRequest.getnewNode(), out);
                out.writeInt(joinRequest.getTTL());
        }

        @Override
        public ForwardJoin deserialize(ByteBuf in) throws IOException {
        	Host node = Host.serializer.deserialize(in);
        	int ttl = in.readInt();
            return new ForwardJoin(node,ttl);
        }
    };

  
}
