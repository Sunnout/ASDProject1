package protocols.broadcast.plumtree.timers;

import java.util.UUID;

import babel.generic.ProtoTimer;

public class MissingMessageTimer extends ProtoTimer {
	public static final short TIMER_ID = 601;
	
	private UUID messageId;

	public MissingMessageTimer(UUID messageId) {
		super(TIMER_ID);
		this.messageId = messageId;
	}

	@Override
	public ProtoTimer clone() {
		return this;
	}
	
	public UUID getMessageId() {
		return this.messageId;
	}
}