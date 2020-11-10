package protocols.broadcast.plumtree.timers;

import babel.generic.ProtoTimer;

public class ClearReceivedMessagesTimer extends ProtoTimer {
	public static final short TIMER_ID = 603;
	
	public ClearReceivedMessagesTimer() {
		super(TIMER_ID);
	}

	@Override
	public ProtoTimer clone() {
		return this;
	}
}