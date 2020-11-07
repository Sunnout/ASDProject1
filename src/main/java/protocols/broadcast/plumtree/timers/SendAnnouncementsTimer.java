package protocols.broadcast.plumtree.timers;

import babel.generic.ProtoTimer;

public class SendAnnouncementsTimer extends ProtoTimer {
	public static final short TIMER_ID = 602;
	
	public SendAnnouncementsTimer() {
		super(TIMER_ID);
	}

	@Override
	public ProtoTimer clone() {
		return this;
	}
}