package protocols.membership.hyparview.messages;

import java.io.IOException;
import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

public class NeighborRequest extends ProtoMessage {

    public final static short MSG_ID = 695;

    private final Host node;
    private final boolean priority;

    public NeighborRequest(Host node, boolean prio) { 
        super(MSG_ID);
        this.node = node;
        this.priority = prio;
    }

    public Host getNode() {
        return node;
    }
    
    public boolean getPriority() {
    	return priority;
    }

    @Override
    public String toString() {
        return "Neighbor Request from  " + node.toString() + " with " + priority + " priority";
    }
    
    public static ISerializer<NeighborRequest> serializer = new ISerializer<NeighborRequest>() {
        @Override
        public void serialize(NeighborRequest neighborRequest, ByteBuf out) throws IOException {
                Host.serializer.serialize(neighborRequest.getNode(), out);
                out.writeBoolean(neighborRequest.getPriority());
        }

        @Override
        public NeighborRequest deserialize(ByteBuf in) throws IOException {
        	Host node = Host.serializer.deserialize(in);
        	boolean prio = in.readBoolean();
            return new NeighborRequest(node,prio);
        }
    };

  
}
