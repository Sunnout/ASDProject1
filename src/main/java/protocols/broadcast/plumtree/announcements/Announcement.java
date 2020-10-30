package protocols.broadcast.plumtree.announcements;

import network.data.Host;

public class Announcement {
	
    private final Host sender;
    private int round;
    
	public Announcement(Host sender, int round) {
		this.sender = sender;
		this.round = round;
	}

	public int getRound() {
		return round;
	}

	public Host getSender() {
		return sender;
	}

}