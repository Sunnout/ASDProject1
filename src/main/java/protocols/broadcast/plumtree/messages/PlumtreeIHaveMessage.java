package protocols.broadcast.plumtree.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class PlumtreeIHaveMessage extends ProtoMessage {
    public static final short MSG_ID = 602;

    private final UUID mid;
    private final Host sender;

    private final short toDeliver;
    private final UUID content; //UUID of message that sender has
    
    private int round;

    @Override
    public String toString() {
        return "PlumtreeIHaveMessage{" +
                "mid=" + mid +
                '}';
    }

    public PlumtreeIHaveMessage(UUID mid, Host sender, int round, short toDeliver, UUID content) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.round = round;
        this.toDeliver = toDeliver;
        this.content = content;
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

    public UUID getContent() {
        return content;
    }

    public static ISerializer<PlumtreeIHaveMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PlumtreeIHaveMessage plumtreeIHaveMessage, ByteBuf out) throws IOException {
            out.writeLong(plumtreeIHaveMessage.mid.getMostSignificantBits());
            out.writeLong(plumtreeIHaveMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(plumtreeIHaveMessage.sender, out);
            out.writeInt(plumtreeIHaveMessage.round);
            out.writeShort(plumtreeIHaveMessage.toDeliver);
            out.writeLong(plumtreeIHaveMessage.content.getMostSignificantBits());
            out.writeLong(plumtreeIHaveMessage.content.getLeastSignificantBits());
        }

        @Override
        public PlumtreeIHaveMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            int round = in.readInt();
            short toDeliver = in.readShort();
            firstLong = in.readLong();
            secondLong = in.readLong();
            UUID content = new UUID(firstLong, secondLong);

            return new PlumtreeIHaveMessage(mid, sender, round, toDeliver, content);
        }
    };
}
