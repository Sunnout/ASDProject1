package protocols.membership.hyparview.messages;

import java.io.IOException;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

public class ShuffleRequestReply extends ProtoMessage {

	
	public final static short MSG_ID = 113;

    private final boolean accepted;
    
    public ShuffleRequestReply( boolean accepted) { 
        super(MSG_ID);
        this.accepted = accepted;
    }

   
    
    public boolean getAccepted() {
    	return accepted;
    }

    @Override
    public String toString() {
        return "Shuffle Request reply from  " + accepted + " request";
    }
    
    public static ISerializer<ShuffleRequestReply> serializer = new ISerializer<ShuffleRequestReply>() {
        @Override
        public void serialize(ShuffleRequestReply neighborRequest, ByteBuf out) throws IOException {
                out.writeBoolean(neighborRequest.getAccepted());
        }

        @Override
        public ShuffleRequestReply deserialize(ByteBuf in) throws IOException {
        	boolean prio = in.readBoolean();
            return new ShuffleRequestReply(prio);
        }
    };

  
}


