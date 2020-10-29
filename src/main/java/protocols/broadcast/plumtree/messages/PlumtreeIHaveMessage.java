package protocols.broadcast.plumtree.messages;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

public class PlumtreeIHaveMessage extends ProtoMessage {
    public static final short MSG_ID = 602;

    private final UUID mid;
    private final Host sender;
    private final Set<UUID> messageIds; // Set of UUIDs of messages that sender has
    private int round;

    @Override
    public String toString() {
        return "PlumtreeIHaveMessage{" +
                "mid=" + mid +
                '}';
    }

    public PlumtreeIHaveMessage(UUID mid, Host sender, int round, Set<UUID> messageIds) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.round = round;
        this.messageIds = messageIds;
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
    
    public Set<UUID> getMessageIds() {
    	return messageIds;
    }
    
    public void addMessageId(UUID messageId) {
    	messageIds.add(messageId);
    }
    
    public boolean removeMessageId(UUID messageId) {
    	return messageIds.remove(messageId);
    }
    
    public void clearMessageIds() {
    	messageIds.clear();
    }

    public static ISerializer<PlumtreeIHaveMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PlumtreeIHaveMessage plumtreeIHaveMessage, ByteBuf out) throws IOException {
            out.writeLong(plumtreeIHaveMessage.mid.getMostSignificantBits());
            out.writeLong(plumtreeIHaveMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(plumtreeIHaveMessage.sender, out);
            out.writeInt(plumtreeIHaveMessage.round);
            out.writeInt(plumtreeIHaveMessage.messageIds.size());
            plumtreeIHaveMessage.messageIds.forEach(id -> {
            	out.writeLong(id.getMostSignificantBits());
                out.writeLong(id.getLeastSignificantBits());
			});

        }

        @Override
        public PlumtreeIHaveMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            int round = in.readInt();
            Set<UUID> messageIds = new HashSet<>();
            for(int i = 0; i < in.readInt(); i++) {
            	messageIds.add(new UUID(in.readLong(), in.readLong()));
            }

            return new PlumtreeIHaveMessage(mid, sender, round, messageIds);
        }
    };
}
