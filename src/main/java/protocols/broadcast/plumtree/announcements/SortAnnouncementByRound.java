package protocols.broadcast.plumtree.announcements;

import java.util.Comparator;

public class SortAnnouncementByRound implements Comparator<Announcement> {

	@Override
	public int compare(Announcement o1, Announcement o2) {

		if (o1.getRound() < o2.getRound())
			return -1;
		else if (o1.getRound() > o2.getRound())
			return 1;
		else
			return 0;
	}

}
