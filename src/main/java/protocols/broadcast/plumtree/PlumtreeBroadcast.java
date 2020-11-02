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
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;

public class PlumtreeBroadcast extends GenericProtocol {
	private static final Logger logger = LogManager.getLogger(PlumtreeBroadcast.class);

	// Protocol information, to register in babel
	public static final String PROTOCOL_NAME = "Plumtree";
	public static final short PROTOCOL_ID = 600;

	// Protocol parameters
	// TODO: como escolher timeout?
	// TODO: how to define threshold value
	public static final int LONGER_MISSING_TIMEOUT = 5000;
	public static final int SHORTER_MISSING_TIMEOUT = 3000;
	public static final int THRESHOLD = 4;


	private final Host myself; // My own address/port
	private final Set<Host> eagerPushPeers; // Neighbours with which to use eager push gossip
	private final Set<Host> lazyPushPeers; // Neighbours with which to use lazy push gossip
	private PlumtreeIHaveMessage lazyIHaveMessage; // IHAVE msg with announcements to be sent
	private final HashMap<UUID, List<Announcement>> missingMessages; // Hashmap of missing msg ids to list of announcements
	private final HashMap<UUID, PlumtreeGossipMessage> received; // Hashmap of UUIDs to received messages
	private final HashMap<UUID, Long> missingMessageTimers; // Map of <msgIds, timerIds> for missing messages
	private boolean channelReady;

	public PlumtreeBroadcast(Properties properties, Host myself) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.myself = myself;
		eagerPushPeers = new HashSet<>();
		lazyPushPeers = new HashSet<>();
		lazyIHaveMessage = new PlumtreeIHaveMessage(UUID.randomUUID(), myself, 0, new HashSet<>());
		missingMessages = new HashMap<>();
		received = new HashMap<>();
		missingMessageTimers = new HashMap<>();
		channelReady = false;

		/*--------------------- Register Request Handlers ----------------------------- */
		registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

		/*-------------------- Register Notification Handlers ------------------------- */
		subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
		subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
		subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);

		/*--------------------- Register Timer Handlers ----------------------------- */
		registerTimerHandler(MissingMessageTimer.TIMER_ID, this::uponMissingMessageTimer);
	}

	@Override
	public void init(Properties props) {
		/*
		 * Init é para inicializar timers ou buscar configs que só estão
		 * definidas depois dos protocolos terem sido todos inicializados
		 */
	}

	// Upon receiving the channelId from the membership, register callbacks and serializers
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
		logger.trace("Received Gossip {} from {}", msg, from);

		UUID mid = msg.getMid();

		if (!received.containsKey(mid)) {
			received.put(mid, msg);
			triggerNotification(new DeliverNotification(mid, from, msg.getContent()));

			if(missingMessages.remove(mid) != null) {
				cancelTimer(missingMessageTimers.get(mid));
			}

			msg.incrementRound();
			eagerPushGossip(msg);
			lazyPushGossip(msg);
			eagerPushPeers.add(from);
			lazyPushPeers.remove(from);
			//optimization(msg, from);

		} else if (!from.equals(myself)) {
			eagerPushPeers.remove(from);
			lazyPushPeers.add(from);
			sendMessage(new PlumtreePruneMessage(UUID.randomUUID(), myself), from);
			logger.info("Sent Prune {} to {}", msg, from);

		}
	}

	private void uponPlumtreeIHaveMessage(PlumtreeIHaveMessage msg, Host from, short sourceProto, int channelId) {
		logger.trace("Received IHave {} from {}", msg, from);

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
		logger.trace("Received Prune {} from {}", msg, from);

		eagerPushPeers.remove(from);
		lazyPushPeers.add(from);
	}

	private void uponPlumtreeGraftMessage(PlumtreeGraftMessage msg, Host from, short sourceProto, int channelId) {
		logger.trace("Received Graft {} from {}", msg, from);

		UUID mid = msg.getMid();
		eagerPushPeers.add(from);
		lazyPushPeers.remove(from);
		if (received.containsKey(mid)) {
			//TODO: esta bem?? ou preciso de criar gossip nova comigo no sender?
			sendMessage(received.get(mid), from);
			logger.info("Sent Gossip {} to {}", msg, from);
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
		UUID mid = missingMessageTimer.getMessageId();
		missingMessageTimers.remove(mid);
		long timer = setupTimer(new MissingMessageTimer(mid), SHORTER_MISSING_TIMEOUT);
		missingMessageTimers.put(mid, timer);

		Announcement announcement = getFirstAnnouncement(mid);

		if(announcement != null) {
			Host sender = announcement.getSender();
			eagerPushPeers.add(sender);
			lazyPushPeers.remove(sender);
			PlumtreeGraftMessage msg = new PlumtreeGraftMessage(UUID.randomUUID(), myself, announcement.getRound(), mid);
			sendMessage(msg, sender);
			logger.info("Sent Graft {} to {}", msg, sender);
		}
	}


	/*----------------------------------- Procedures -------------------------------------- */

	private void eagerPushGossip(PlumtreeGossipMessage msg) {

		eagerPushPeers.forEach(host -> {
			if (!host.equals(myself)) {
				sendMessage(msg, host);
				logger.trace("Sent Gossip {} to {}", msg, host);
			}
		});
	}

	private void lazyPushGossip(PlumtreeGossipMessage msg) {
		lazyIHaveMessage.addMessageId(msg.getMid());
		simpleAnnouncementPolicy();

	}

	private void simpleAnnouncementPolicy() {
		lazyPushPeers.forEach(host -> {
			if (!host.equals(myself)) {
				sendMessage(lazyIHaveMessage, host);
				logger.info("Sent IHave {} to {}", lazyIHaveMessage, host);
			}
		});

		lazyIHaveMessage = new PlumtreeIHaveMessage(UUID.randomUUID(), myself, 0, new HashSet<>());
	}

	private void notSoSimpleAnnouncementPolicy() {
		lazyPushPeers.forEach(host -> {
			if (!host.equals(myself)) {
				sendMessage(lazyIHaveMessage, host);
				logger.info("Sent IHave {} to {}", lazyIHaveMessage, host);
			}
		});

		lazyIHaveMessage = new PlumtreeIHaveMessage(UUID.randomUUID(), myself, 0, new HashSet<>());

		/*
		 * TODO: Juntar lista de uuids na ihavemessage e criar um timer que
		 * periodicamente envia todas as que existem mas, se houverem
		 * mudanças no lazyPushPeers (add ou remove) tem de se enviar
		 * tudo antes de adicionar ou remover. Criar um boolean que diz
		 * se naquele periodo do timer já enviou entretanto sem ser por
		 * culpa do timer, se sim, não envia, senão envia.
		 */
	}

	private void optimization(PlumtreeGossipMessage msg, Host from) {
		Announcement announcement = getFirstAnnouncement(msg.getMid());

		if(announcement != null) {
			int r = announcement.getRound();
			int round = msg.getRound()-1;
			Host sender = announcement.getSender();
			if(r < round && (round - r) >= THRESHOLD) {
				PlumtreeGraftMessage graftMsg = new PlumtreeGraftMessage(UUID.randomUUID(), myself, r, null);
				sendMessage(graftMsg, sender);
				logger.info("Sent Graft {} to {}", graftMsg, sender);

				PlumtreePruneMessage pruneMsg = new PlumtreePruneMessage(UUID.randomUUID(), myself);
				sendMessage(pruneMsg, from);
				logger.info("Sent Prune {} to {}", pruneMsg, from);
			}
		}
	}

	private Announcement getFirstAnnouncement(UUID mid) {
		Announcement a = null;
		List<Announcement> announcementList = missingMessages.get(mid);
		if(announcementList != null) {
			announcementList.sort(new SortAnnouncementByRound());
			a = announcementList.get(0);
		}
		return a;
	}

}
