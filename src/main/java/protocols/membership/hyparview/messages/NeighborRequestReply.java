package protocols.membership.hyparview.messages;

import java.io.IOException;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

public class NeighborRequestReply extends ProtoMessage {

	
	public final static short MSG_ID = 696;

    private final boolean accepted;
    
    public NeighborRequestReply( boolean accepted) { 
        super(MSG_ID);
        this.accepted = accepted;
    }

   
    
    public boolean getAccepted() {
    	return accepted;
    }

    @Override
    public String toString() {
        return "Neighbor Request reply from  " + accepted + " request";
    }
    
    public static ISerializer<NeighborRequestReply> serializer = new ISerializer<NeighborRequestReply>() {
        @Override
        public void serialize(NeighborRequestReply neighborRequest, ByteBuf out) throws IOException {
                out.writeBoolean(neighborRequest.getAccepted());
        }

        @Override
        public NeighborRequestReply deserialize(ByteBuf in) throws IOException {
        	boolean prio = in.readBoolean();
            return new NeighborRequestReply(prio);
        }
    };

  
}


