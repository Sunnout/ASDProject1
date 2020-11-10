package protocols.broadcast.plumtree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import babel.generic.ProtoMessage;
import network.data.Host;
import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.broadcast.plumtree.announcements.Announcement;
import protocols.broadcast.plumtree.announcements.SortAnnouncementByRound;
import protocols.broadcast.plumtree.messages.PlumtreeGossipMessage;
import protocols.broadcast.plumtree.messages.PlumtreeGraftMessage;
import protocols.broadcast.plumtree.messages.PlumtreeIHaveMessage;
import protocols.broadcast.plumtree.messages.PlumtreePruneMessage;
import protocols.broadcast.plumtree.timers.ClearReceivedMessagesTimer;
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

	private final Host myself; // My own address/port
	private final Set<Host> neighbours; // Known neighbours
	private final Set<Host> eagerPushPeers; // Neighbours with which to use eager push gossip
	private final Set<Host> lazyPushPeers; // Neighbours with which to use lazy push gossip
	private PlumtreeIHaveMessage lazyIHaveMessage; // IHAVE msg with announcements to be sent
	private final Map<UUID, List<Announcement>> missingMessages; // Map of <msgIds, list of announcements>
	private final Map<UUID, PlumtreeGossipMessage> received; // Map of <msgIds, receivedMessages>
	private final Map<UUID, Long> receivedTimes; // Map of <msgIds, receivedTimes>
	private final Map<UUID, Long> missingMessageTimers; // Map of <msgIds, timerIds> for missing messages
	private final Set<UUID> alreadyGrafted; // Ids of messages that were grafted
	private boolean sentAnnouncements; // If announcements were sent within this period
	
	// Protocol parameters
	private final int clearReceivedTimeout; // Timeout to clear received messages
	private final int announcementTimeout; // Timeout to send compact announcements
	private final int longerMissingTimeout; // Longer timeout to graft missing message
	private final int shorterMissingTimeout; // Shorter timeout to graft missing message
	private final int optimizationThreshold; // Threshold to perform optimization
	
	// Message counters
	private int nSentGossipMsgs;
	private int nSentGraftMsgs;
	private int nSentPruneMsgs;
	private int nSentIHaveMsgs;
	private int nReceivedGossipMsgs;
	private int nReceivedGraftMsgs;
	private int nReceivedPruneMsgs;
	private int nReceivedIHaveMsgs;

	private boolean channelReady;

	public PlumtreeBroadcast(Properties properties, Host myself) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.myself = myself;
		
		neighbours = new HashSet<>();
		eagerPushPeers = new HashSet<>();
		lazyPushPeers = new HashSet<>();
		lazyIHaveMessage = new PlumtreeIHaveMessage(UUID.randomUUID(), myself, 0, new HashSet<>());
		missingMessages = new HashMap<>();
		received = new HashMap<>();
		receivedTimes = new HashMap<>();
		missingMessageTimers = new HashMap<>();
		alreadyGrafted = new HashSet<>();
		sentAnnouncements = false;
		
		// Get some configurations from properties file
		clearReceivedTimeout = Integer.parseInt(properties.getProperty("clear_received_time", "5000"));
		announcementTimeout = Integer.parseInt(properties.getProperty("announcement_timeout", "2000"));
		longerMissingTimeout = Integer.parseInt(properties.getProperty("longer_missing_timeout", "500"));
		shorterMissingTimeout = Integer.parseInt(properties.getProperty("shorter_missing_timeout", "400"));
		optimizationThreshold = Integer.parseInt(properties.getProperty("optimization_threshold", "3"));

		nSentGossipMsgs = 0;
		nSentGraftMsgs = 0;
		nSentPruneMsgs = 0;
		nSentIHaveMsgs = 0;
		nReceivedGossipMsgs = 0;
		nReceivedGraftMsgs = 0;
		nReceivedPruneMsgs = 0;
		nReceivedIHaveMsgs = 0;
		
		channelReady = false;


		/*--------------------- Register Request Handlers ----------------------------- */
		registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

		/*-------------------- Register Notification Handlers ------------------------- */
		subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
		subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
		subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);

		/*--------------------- Register Timer Handlers ----------------------------- */
		registerTimerHandler(MissingMessageTimer.TIMER_ID, this::uponMissingMessageTimer);
		registerTimerHandler(SendAnnouncementsTimer.TIMER_ID, this::uponSendAnnouncementsTimer);
		registerTimerHandler(ClearReceivedMessagesTimer.TIMER_ID, this::uponClearReceivedMessagesTimer);
	}

	@Override
	public void init(Properties props) {
		//Setup the timer used to send compact announcements
		setupPeriodicTimer(new SendAnnouncementsTimer(), announcementTimeout, announcementTimeout);
		setupPeriodicTimer(new ClearReceivedMessagesTimer(), clearReceivedTimeout, clearReceivedTimeout);
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
		long receivedTime = System.currentTimeMillis();
		PlumtreeGossipMessage msg = new PlumtreeGossipMessage(request.getMsgId(), request.getSender(), 0, request.getMsg());
		eagerPushGossip(msg);
		lazyPushGossip(msg);
		triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));
		received.put(msg.getMid(), msg);
		receivedTimes.put(msg.getMid(), receivedTime);
	}


	/*--------------------------------- Messages ---------------------------------------- */

	private void uponPlumtreeGossipMessage(PlumtreeGossipMessage msg, Host from, short sourceProto, int channelId) {
		long receivedTime = System.currentTimeMillis();
		logger.info("Received Gossip {} from {}", msg, from);
		nReceivedGossipMsgs++;
		UUID mid = msg.getMid();

		if (!received.containsKey(mid)) {
			received.put(mid, msg);
			receivedTimes.put(mid, receivedTime);
			triggerNotification(new DeliverNotification(mid, from, msg.getContent()));

			if(missingMessages.remove(mid) != null) {
				cancelTimer(missingMessageTimers.get(mid));
				alreadyGrafted.remove(mid);
			}

			msg.incrementRound();
			eagerPushGossip(msg);
			lazyPushGossip(msg);
			if(neighbours.contains(from)) {
				eagerPushPeers.add(from);
				lazyPushPeers.remove(from);
				//optimization(msg, from);
			}
		} else if (neighbours.contains(from) && !from.equals(myself) && eagerPushPeers.size() > 1) {
			eagerPushPeers.remove(from);
			// Send announcements before adding new lazy push neighbour 
			notSoSimpleAnnouncementPolicy();
			lazyPushPeers.add(from);

			PlumtreePruneMessage pruneMsg = new PlumtreePruneMessage(UUID.randomUUID(), myself);
			sendMessage(pruneMsg, from);
			logger.info("Sent Prune {} to {}", pruneMsg, from);
			nSentPruneMsgs++;
		}
	}

	private void uponPlumtreeIHaveMessage(PlumtreeIHaveMessage msg, Host from, short sourceProto, int channelId) {
		logger.info("Received IHave {} from {}", msg, from);
		nReceivedIHaveMsgs++;

		msg.getMessageIds().forEach(id -> {
			if (!received.containsKey(id) && !missingMessageTimers.containsKey(id)) {
				List<Announcement> announcements = new ArrayList<Announcement>();
				announcements.add(new Announcement(from, msg.getRound()));
				missingMessages.put(id, announcements);
				long timer = setupTimer(new MissingMessageTimer(id), longerMissingTimeout);
				missingMessageTimers.put(id, timer);
			}
		});

	}

	private void uponPlumtreePruneMessage(PlumtreePruneMessage msg, Host from, short sourceProto, int channelId) {
		logger.info("Received Prune {} from {}", msg, from);
		nReceivedPruneMsgs++;
		
		if(neighbours.contains(from)) {
			eagerPushPeers.remove(from);
			// Send announcements before adding new lazy push neighbour 
			notSoSimpleAnnouncementPolicy();
			lazyPushPeers.add(from);
		}
	}

	private void uponPlumtreeGraftMessage(PlumtreeGraftMessage msg, Host from, short sourceProto, int channelId) {
		UUID mid = msg.getMessageId();
		logger.info("Received Graft {} from {}", mid, from);
		nReceivedGraftMsgs++;
		
		if(neighbours.contains(from)) {
			eagerPushPeers.add(from);
			lazyPushPeers.remove(from);
			if (received.containsKey(mid)) {
				sendMessage(received.get(mid), from);
				logger.info("Sent Gossip {} to {}", mid, from);
				nSentGossipMsgs++;
			}
		}
	}

	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}


	/*--------------------------------- Notifications ---------------------------------------- */

	private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
		for (Host h : notification.getNeighbours()) {
			neighbours.add(h);
			eagerPushPeers.add(h);
			logger.info("New neighbour: " + h);
		}
	}

	private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
		for (Host h : notification.getNeighbours()) {
			neighbours.remove(h);
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
		long timer = setupTimer(new MissingMessageTimer(mid), shorterMissingTimeout);
		missingMessageTimers.put(mid, timer);

		// Only send graft is it wasn't sent already and then remove it from the already grafted
		if(!alreadyGrafted.remove(mid)) {
			Announcement announcement = removeFirstAnnouncement(mid);

			if(announcement != null) {
				Host sender = announcement.getSender();
				if(neighbours.contains(sender)) {
					eagerPushPeers.add(sender);
					lazyPushPeers.remove(sender);

					PlumtreeGraftMessage graftMsg = new PlumtreeGraftMessage(UUID.randomUUID(), myself, announcement.getRound(), mid);
					sendMessage(graftMsg, sender);
					logger.info("Sent Graft {} to {}", mid, sender);
					nSentGraftMsgs++;

					sendGraftsForAllAnnouncements(sender);
				}
			}
		}
	}

	private void uponSendAnnouncementsTimer(SendAnnouncementsTimer sendAnnouncementsTimer, long timerId) {
		// Send announcements only if they weren't sent before in this period
		if(!sentAnnouncements)
			notSoSimpleAnnouncementPolicy();

		sentAnnouncements = false;
	}

	private void uponClearReceivedMessagesTimer(ClearReceivedMessagesTimer clearReceivedMessagesTimer, long timerId) {
		Iterator<Entry<UUID, PlumtreeGossipMessage>> it = received.entrySet().iterator();
		while (it.hasNext()) {
			Entry<UUID, PlumtreeGossipMessage> entry = it.next();
			if(System.currentTimeMillis() > receivedTimes.get(entry.getKey()) + clearReceivedTimeout) {
				receivedTimes.remove(entry.getKey());
				it.remove();
			}
		}
	}


	/*----------------------------------- Procedures -------------------------------------- */

	private void eagerPushGossip(PlumtreeGossipMessage msg) {

		eagerPushPeers.forEach(host -> {
			if (!host.equals(myself)) {
				sendMessage(msg, host);
				logger.info("Sent Gossip {} to {}", msg, host);
				nSentGossipMsgs++;
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
					nSentIHaveMsgs++;
				}
			});

			lazyIHaveMessage = new PlumtreeIHaveMessage(UUID.randomUUID(), myself, 0, new HashSet<>());
			sentAnnouncements = true;
		}
	}

	private void optimization(PlumtreeGossipMessage msg, Host from) {
		Announcement announcement = getFirstAnnouncement(msg.getMid());

		if(announcement != null) {
			int r = announcement.getRound();
			int round = msg.getRound()-1;
			Host sender = announcement.getSender();
			if(r < round && (round - r) >= optimizationThreshold && neighbours.contains(sender)) {
				PlumtreeGraftMessage graftMsg = new PlumtreeGraftMessage(UUID.randomUUID(), myself, r, null);
				sendMessage(graftMsg, sender);
				logger.info("Sent Graft {} to {}", null, sender);
				nSentGraftMsgs++;

				PlumtreePruneMessage pruneMsg = new PlumtreePruneMessage(UUID.randomUUID(), myself);
				sendMessage(pruneMsg, from);
				logger.info("Sent Prune {} to {}", pruneMsg, from);
				nSentPruneMsgs++;
			}
		}
	}
	
	private void sendGraftsForAllAnnouncements(Host host) {
		// Send graft for all the announcements of host
		missingMessages.forEach((msgId, list) -> {
			if(!alreadyGrafted.contains(msgId)) {
				Iterator<Announcement> i = list.iterator();
				while (i.hasNext()) {
					Announcement a = i.next();
					if(a.getSender().equals(host)) {
						PlumtreeGraftMessage otherGraftMsg = new PlumtreeGraftMessage(UUID.randomUUID(), myself, a.getRound(), msgId);
						sendMessage(otherGraftMsg, host);
						logger.info("Sent Graft {} to {}", msgId, host);
						nSentGraftMsgs++;
						alreadyGrafted.add(msgId);
						i.remove();
					}
				}
			}
		});
	}

	private Announcement getFirstAnnouncement(UUID mid) {
		Announcement a = null;
		List<Announcement> announcementList = missingMessages.get(mid);
		if(announcementList != null) {
			announcementList.sort(new SortAnnouncementByRound());
			if(announcementList.size() > 0)
				a = announcementList.get(0);
		}
		return a;
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
	
	/*----------------------------------- Metrics -------------------------------------- */
	
	public void printMetrics() {
		System.out.println("Sent Gossip Msgs: " + nSentGossipMsgs);
		System.out.println("Sent Graft Msgs: " + nSentGraftMsgs);
		System.out.println("Sent Prune Msgs: " + nSentPruneMsgs);
		System.out.println("Sent IHave Msgs: " + nSentIHaveMsgs);
		System.out.println("Received Gossip Msgs: " + nReceivedGossipMsgs);
		System.out.println("Received Graft Msgs: " + nReceivedGraftMsgs);
		System.out.println("Received Prune Msgs: " + nReceivedPruneMsgs);
		System.out.println("Received IHave Msgs: " + nReceivedIHaveMsgs); 
	}

}
