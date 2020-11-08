package protocols.broadcast.plumtree.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class PlumtreePruneMessage extends ProtoMessage {
	public static final short MSG_ID = 603;

	private final UUID mid;
	private final Host sender;

	@Override
	public String toString() {
		return "PlumtreePruneMessage{" + "mid=" + mid + '}';
	}

	public PlumtreePruneMessage(UUID mid, Host sender) {
		super(MSG_ID);
		this.mid = mid;
		this.sender = sender;
	}

	public Host getSender() {
		return sender;
	}

	public UUID getMid() {
		return mid;
	}

	public static ISerializer<PlumtreePruneMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(PlumtreePruneMessage plumtreePruneMessage, ByteBuf out) throws IOException {
			out.writeLong(plumtreePruneMessage.mid.getMostSignificantBits());
			out.writeLong(plumtreePruneMessage.mid.getLeastSignificantBits());
			Host.serializer.serialize(plumtreePruneMessage.sender, out);
		}

		@Override
		public PlumtreePruneMessage deserialize(ByteBuf in) throws IOException {
			long firstLong = in.readLong();
			long secondLong = in.readLong();
			UUID mid = new UUID(firstLong, secondLong);
			Host sender = Host.serializer.deserialize(in);

			return new PlumtreePruneMessage(mid, sender);
		}
	};
}
