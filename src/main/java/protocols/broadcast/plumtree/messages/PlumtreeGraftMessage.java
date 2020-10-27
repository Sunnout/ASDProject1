package protocols.broadcast.plumtree.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class PlumtreeGraftMessage extends ProtoMessage {
    public static final short MSG_ID = 604;

    private final UUID mid;
    private final Host sender;

    private final short toDeliver;
    private final UUID messageId;
    
    private int round;

    @Override
    public String toString() {
        return "PlumtreeGraftMessage{" +
                "mid=" + mid +
                '}';
    }

    public PlumtreeGraftMessage(UUID mid, Host sender, int round, short toDeliver, UUID messageId) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.round = 0;
        this.toDeliver = toDeliver;
        this.messageId = messageId;
    }

    public int getRound() {
		return round;
	}
    
    public void incrementRound() {
		this.round++;
	}


	public Host getSender() {
        return sender;
    }

    public UUID getMid() {
        return mid;
    }

    public short getToDeliver() {
        return toDeliver;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public static ISerializer<PlumtreeGraftMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PlumtreeGraftMessage plumtreeGraftMessage, ByteBuf out) throws IOException {
            out.writeLong(plumtreeGraftMessage.mid.getMostSignificantBits());
            out.writeLong(plumtreeGraftMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(plumtreeGraftMessage.sender, out);
            out.writeInt(plumtreeGraftMessage.round);
            out.writeShort(plumtreeGraftMessage.toDeliver);
            out.writeLong(plumtreeGraftMessage.messageId.getMostSignificantBits());
            out.writeLong(plumtreeGraftMessage.messageId.getLeastSignificantBits());
        }

        @Override
        public PlumtreeGraftMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            int round = in.readInt();
            short toDeliver = in.readShort();
            firstLong = in.readLong();
            secondLong = in.readLong();
            UUID messageId = new UUID(firstLong, secondLong);

            return new PlumtreeGraftMessage(mid, sender, round, toDeliver, messageId);
        }
    };
}
