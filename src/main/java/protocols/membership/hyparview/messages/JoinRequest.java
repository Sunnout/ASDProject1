package protocols.membership.hyparview.messages;

import java.io.IOException;


import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

public class JoinRequest extends ProtoMessage {

    public final static short MSG_ID = 111;


    public JoinRequest() {
        super(MSG_ID);
    }

   
    @Override
    public String toString() {
        return "Join Request ";
    }
    
    public static ISerializer<JoinRequest> serializer = new ISerializer<JoinRequest>() {
        @Override
        public void serialize(JoinRequest joinRequest, ByteBuf out) throws IOException {
        }

        @Override
        public JoinRequest deserialize(ByteBuf in) throws IOException {
            return new JoinRequest();
        }
    };

  
}
