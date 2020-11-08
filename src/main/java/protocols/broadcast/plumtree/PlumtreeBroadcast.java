package protocols.broadcast.plumtree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import babel.generic.ProtoMessage;
import channel.tcp.events.ChannelMetrics;
import network.data.Host;
import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.broadcast.plumtree.announcements.Announcement;
import protocols.broadcast.plumtree.announcements.SortAnnouncementByRound;
import protocols.broadcast.plumtree.messages.PlumtreeGossipMessage;
import protocols.broadcast.plumtree.messages.PlumtreeGraftMessage;
import protocols.broadcast.plumtree.messages.PlumtreeIHaveMessage;
import protocols.broadcast.plumtree.messages.PlumtreePruneMessage;
import protocols.broadcast.plumtree.timers.MissingMessageTimer;
import protocols.broadcast.plumtree.timers.SendAnnouncementsTimer;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;

public class PlumtreeBroadcast extends GenericProtocol {
	private static final Logger logger = LogManager.getLogger(PlumtreeBroadcast.class);
	

	// Protocol information, to register in babel
	public static final String PROTOCOL_NAME = "Plumtree";
	public static final short PROTOCOL_ID = 600;

	// Protocol parameters
	public static final int ANNOUNCEMENT_TIMEOUT = 2000;
	// TODO: define announce timeout value and maybe put in props?
	public static final int LONGER_MISSING_TIMEOUT = 500;
	public static final int SHORTER_MISSING_TIMEOUT = 400;
	public static final int THRESHOLD = 3;

	private final Host myself; // My own address/port
	private final Set<Host> eagerPushPeers; // Neighbours with which to use eager push gossip
	private final Set<Host> lazyPushPeers; // Neighbours with which to use lazy push gossip
	private PlumtreeIHaveMessage lazyIHaveMessage; // IHAVE msg with announcements to be sent
	private final HashMap<UUID, List<Announcement>> missingMessages; // Hashmap of missing msg ids to list of announcements
	private final HashMap<UUID, PlumtreeGossipMessage> received; // Hashmap of UUIDs to received messages
	private final HashMap<UUID, Long> missingMessageTimers; // Map of <msgIds, timerIds> for missing messages
	private final Set<UUID> alreadyGrafted;
	private boolean channelReady;
	private boolean sentAnnouncements;

	public PlumtreeBroadcast(Properties properties, Host myself) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.myself = myself;
		eagerPushPeers = new HashSet<>();
		lazyPushPeers = new HashSet<>();
		lazyIHaveMessage = new PlumtreeIHaveMessage(UUID.randomUUID(), myself, 0, new HashSet<>());
		missingMessages = new HashMap<>();
		received = new HashMap<>();
		missingMessageTimers = new HashMap<>();
		alreadyGrafted = new HashSet<>();
		channelReady = false;
		sentAnnouncements = false;

		/*--------------------- Register Request Handlers ----------------------------- */
		registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

		/*-------------------- Register Notification Handlers ------------------------- */
		subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
		subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
		subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);

		/*--------------------- Register Timer Handlers ----------------------------- */
		registerTimerHandler(MissingMessageTimer.TIMER_ID, this::uponMissingMessageTimer);
		registerTimerHandler(SendAnnouncementsTimer.TIMER_ID, this::uponSendAnnouncementsTimer);
	}

	@Override
	public void init(Properties props) {
		//Setup the timer used to send compact announcements
		setupPeriodicTimer(new SendAnnouncementsTimer(), ANNOUNCEMENT_TIMEOUT, ANNOUNCEMENT_TIMEOUT);
	}

	private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
		int cId = notification.getChannelId();
		registerSharedChannel(cId);

		/*---------------------- Register Message Serializers ---------------------- */
		registerMessageSerializer(cId, PlumtreeGossipMessage.MSG_ID, PlumtreeGossipMessage.serializer);
		registerMessageSerializer(cId, PlumtreeIHaveMessage.MSG_ID, PlumtreeIHaveMessage.serializer);
		registerMessageSerializer(cId, PlumtreePruneMessage.MSG_ID, PlumtreePruneMessage.serializer);
		registerMessageSerializer(cId, PlumtreeGraftMessage.MSG_ID, PlumtreeGraftMessage.serializer);

		/*---------------------- Register Message Handlers -------------------------- */

		try {

			registerMessageHandler(cId, PlumtreeGossipMessage.MSG_ID, this::uponPlumtreeGossipMessage, this::uponMsgFail);
			registerMessageHandler(cId, PlumtreeIHaveMessage.MSG_ID, this::uponPlumtreeIHaveMessage, this::uponMsgFail);
			registerMessageHandler(cId, PlumtreePruneMessage.MSG_ID, this::uponPlumtreePruneMessage, this::uponMsgFail);
			registerMessageHandler(cId, PlumtreeGraftMessage.MSG_ID, this::uponPlumtreeGraftMessage, this::uponMsgFail);

		} catch (HandlerRegistrationException e) {
			logger.error("Error registering message handler: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}

		channelReady = true;
	}

	/*--------------------------------- Requests ---------------------------------------- */

	private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
		if (!channelReady)
			return;

		PlumtreeGossipMessage msg = new PlumtreeGossipMessage(request.getMsgId(), request.getSender(), 0, request.getMsg());
		eagerPushGossip(msg);
		lazyPushGossip(msg);
		triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));
		received.put(msg.getMid(), msg);
	}


	/*--------------------------------- Messages ---------------------------------------- */

	private void uponPlumtreeGossipMessage(PlumtreeGossipMessage msg, Host from, short sourceProto, int channelId) {
		logger.info("Received Gossip {} from {}", msg, from);
		//System.out.println("Round: " + msg.getRound());
		UUID mid = msg.getMid();

		if (!received.containsKey(mid)) {
			received.put(mid, msg);
			triggerNotification(new DeliverNotification(mid, from, msg.getContent()));

			if(missingMessages.remove(mid) != null) {
				cancelTimer(missingMessageTimers.get(mid));
				alreadyGrafted.remove(mid);
			}

			msg.incrementRound();
			eagerPushGossip(msg);
			lazyPushGossip(msg);
			eagerPushPeers.add(from);
			lazyPushPeers.remove(from);
			//optimization(msg, from);

		} else if (!from.equals(myself) && eagerPushPeers.size() > 1) {
			eagerPushPeers.remove(from);
			// Send announcements before adding new lazy push neighbour 
			notSoSimpleAnnouncementPolicy();
			lazyPushPeers.add(from);

			PlumtreePruneMessage pruneMsg = new PlumtreePruneMessage(UUID.randomUUID(), myself);
			sendMessage(pruneMsg, from);
			logger.info("Sent Prune {} to {}", pruneMsg, from);

		}
	}

	private void uponPlumtreeIHaveMessage(PlumtreeIHaveMessage msg, Host from, short sourceProto, int channelId) {
		logger.info("Received IHave {} from {}", msg, from);

		msg.getMessageIds().forEach(id -> {
			if (!received.containsKey(id) && !missingMessageTimers.containsKey(id)) {
				List<Announcement> announcements = new ArrayList<Announcement>();
				announcements.add(new Announcement(from, msg.getRound()));
				missingMessages.put(id, announcements);
				long timer = setupTimer(new MissingMessageTimer(id), LONGER_MISSING_TIMEOUT);
				missingMessageTimers.put(id, timer);
			}
		});

	}

	private void uponPlumtreePruneMessage(PlumtreePruneMessage msg, Host from, short sourceProto, int channelId) {
		logger.info("Received Prune {} from {}", msg, from);

		eagerPushPeers.remove(from);
		// Send announcements before adding new lazy push neighbour 
		notSoSimpleAnnouncementPolicy();
		lazyPushPeers.add(from);
	}

	private void uponPlumtreeGraftMessage(PlumtreeGraftMessage msg, Host from, short sourceProto, int channelId) {
		UUID mid = msg.getMessageId();
		logger.info("Received Graft {} from {}", mid, from);

		eagerPushPeers.add(from);
		lazyPushPeers.remove(from);
		if (received.containsKey(mid)) {
			sendMessage(received.get(mid), from);
			logger.info("Sent Gossip {} to {}", mid, from);
		}
	}

	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}


	/*--------------------------------- Notifications ---------------------------------------- */

	private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
		for (Host h : notification.getNeighbours()) {
			eagerPushPeers.add(h);
			logger.info("New neighbour: " + h);
		}
	}

	private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
		for (Host h : notification.getNeighbours()) {
			eagerPushPeers.remove(h);
			lazyPushPeers.remove(h);

			missingMessages.values().forEach(list -> {
				list.forEach(announcement -> {
					if(announcement.getSender().equals(h))
						list.remove(announcement);
				});
			});

			logger.info("Neighbour down: " + h);
		}
	}


	/*--------------------------------- Timers ---------------------------------------- */

	private void uponMissingMessageTimer(MissingMessageTimer missingMessageTimer, long timerId) {
		// Delete current timer and create smaller one
		UUID mid = missingMessageTimer.getMessageId();
		missingMessageTimers.remove(mid);
		long timer = setupTimer(new MissingMessageTimer(mid), SHORTER_MISSING_TIMEOUT);
		missingMessageTimers.put(mid, timer);


		// Only send graft is it wasn't sent already and then remove it from the already grafted
		if(!alreadyGrafted.remove(mid)) {
			Announcement announcement = removeFirstAnnouncement(mid);

			if(announcement != null) {
				Host sender = announcement.getSender();
				eagerPushPeers.add(sender);
				lazyPushPeers.remove(sender);

				PlumtreeGraftMessage graftMsg = new PlumtreeGraftMessage(UUID.randomUUID(), myself, announcement.getRound(), mid);
				sendMessage(graftMsg, sender);
				logger.info("Sent Graft {} to {}", mid, sender);
				alreadyGrafted.add(mid);

				// Send graft for all the announcements of sender
				missingMessages.forEach((msgId, list) -> {
					if(!alreadyGrafted.contains(msgId)) {
						list.forEach(a -> {
							if(a.getSender().equals(sender)) {
								PlumtreeGraftMessage otherGraftMsg = new PlumtreeGraftMessage(UUID.randomUUID(), myself, a.getRound(), msgId);
								sendMessage(otherGraftMsg, sender);
								logger.info("Sent Graft {} to {}", msgId, sender);
								alreadyGrafted.add(msgId);
							}
						});
					}
				});
			}
		}
	}

	private void uponSendAnnouncementsTimer(SendAnnouncementsTimer sendAnnouncementsTimer, long timerId) {
		// Send announcements only if they weren't sent before in this period
		if(!sentAnnouncements)
			notSoSimpleAnnouncementPolicy();

		sentAnnouncements = false;
	}


	/*----------------------------------- Procedures -------------------------------------- */

	private void eagerPushGossip(PlumtreeGossipMessage msg) {

		eagerPushPeers.forEach(host -> {
			if (!host.equals(myself)) {
				sendMessage(msg, host);
				logger.info("Sent Gossip {} to {}", msg, host);
			}
		});
	}

	private void lazyPushGossip(PlumtreeGossipMessage msg) {
		lazyIHaveMessage.addMessageId(msg.getMid());
	}

	private void notSoSimpleAnnouncementPolicy() {
		if(!lazyIHaveMessage.getMessageIds().isEmpty()) {
			lazyPushPeers.forEach(host -> {
				if (!host.equals(myself)) {
					sendMessage(lazyIHaveMessage, host);
					logger.info("Sent IHave {} to {}", lazyIHaveMessage, host);
				}
			});

			lazyIHaveMessage = new PlumtreeIHaveMessage(UUID.randomUUID(), myself, 0, new HashSet<>());
			sentAnnouncements = true;
		}
	}

	private void optimization(PlumtreeGossipMessage msg, Host from) {
		Announcement announcement = removeFirstAnnouncement(msg.getMid());

		if(announcement != null) {
			int r = announcement.getRound();
			int round = msg.getRound()-1;
			Host sender = announcement.getSender();
			if(r < round && (round - r) >= THRESHOLD) {
				PlumtreeGraftMessage graftMsg = new PlumtreeGraftMessage(UUID.randomUUID(), myself, r, null);
				sendMessage(graftMsg, sender);
				logger.info("Sent Graft {} to {}", null, sender);

				PlumtreePruneMessage pruneMsg = new PlumtreePruneMessage(UUID.randomUUID(), myself);
				sendMessage(pruneMsg, from);
				logger.info("Sent Prune {} to {}", pruneMsg, from);
			}
		}
	}

	private Announcement removeFirstAnnouncement(UUID mid) {
		Announcement a = null;
		List<Announcement> announcementList = missingMessages.get(mid);
		if(announcementList != null) {
			announcementList.sort(new SortAnnouncementByRound());
			if(announcementList.size() > 0)
				a = announcementList.remove(0);
		}
		return a;
	}
}
