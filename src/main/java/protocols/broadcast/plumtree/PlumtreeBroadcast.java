package protocols.broadcast.plumtree;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
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

	// TODO: como escolher timeout?
	public static final int LONGER_MISSING_TIMEOUT = 5000;
	public static final int SHORTER_MISSING_TIMEOUT = 3000;
	// TODO: how to define threshold value
	public static final int THRESHOLD = 4;


	private final Host myself; // My own address/port
	private final Set<Host> eagerPushPeers; // Neighbours with which to use eager push gossip
	private final Set<Host> lazyPushPeers; // Neighbours with which to use lazy push gossip
	private final PlumtreeIHaveMessage lazyIHaveMessage; // IHAVE msg with announcements to be sent
	private final Set<PlumtreeIHaveMessage> missing; // Set of IHAVE msgs that contain UUIDs of missing msgs
	private final Set<UUID> received; // Set of UUIDs of received messages
	private final HashMap<UUID, Long> missingMessageTimers; // Map of <msgIds, timerIds> for missing messages


	private boolean channelReady;

	public PlumtreeBroadcast(Properties properties, Host myself) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.myself = myself;
		eagerPushPeers = new HashSet<>();
		lazyPushPeers = new HashSet<>();
		lazyIHaveMessage = new PlumtreeIHaveMessage(UUID.randomUUID(), myself, 0, new HashSet<>());
		missing = new HashSet<>(); 
		received = new HashSet<>();
		missingMessageTimers = new HashMap<>();
		channelReady = false;

		/*--------------------- Register Request Handlers ----------------------------- */
		registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

		/*--------------------- Register Notification Handlers ----------------------------- */
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
			registerMessageHandler(cId, PlumtreeGossipMessage.MSG_ID, this::uponPlumtreeGossipMessage,
					this::uponMsgFail);
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
		received.add(msg.getMid());

	}


	/*--------------------------------- Messages ---------------------------------------- */

	private void uponPlumtreeGossipMessage(PlumtreeGossipMessage msg, Host from, short sourceProto, int channelId) {
		logger.trace("Received {} from {}", msg, from);

		if (received.add(msg.getMid())) {
			triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));

			// Pq só há um timer e só pode cancelar uma vez?
			boolean cancelled = false;
			missing.forEach(iHaveMsg -> {
				if(iHaveMsg.removeMessageId(msg.getMid()) && !cancelled) {
			        cancelTimer(missingMessageTimers.get(msg.getMid()));
				}
			});

			msg.incrementRound();
			eagerPushGossip(msg);
			lazyPushGossip(msg);
			eagerPushPeers.add(msg.getSender());
			lazyPushPeers.remove(msg.getSender());
			optimization(msg);

		} else {
			eagerPushPeers.remove(msg.getSender());
			lazyPushPeers.add(msg.getSender());
			sendMessage(new PlumtreePruneMessage(UUID.randomUUID(), myself), msg.getSender());
		}
	}

	private void uponPlumtreeIHaveMessage(PlumtreeIHaveMessage msg, Host from, short sourceProto, int channelId) {
		// TODO: isto esta correto?
		msg.getMessageIds().forEach(id -> {
			if (received.contains(id)) {
				msg.removeMessageId(id);
			} else if (!missingMessageTimers.containsKey(id)) {
				long timer = setupTimer(new MissingMessageTimer(id), LONGER_MISSING_TIMEOUT);
				missingMessageTimers.put(id, timer);
			}
		});

		if(msg.getMessageIds().size() > 0)
			missing.add(msg);
	}

	private void uponPlumtreePruneMessage(PlumtreePruneMessage msg, Host from, short sourceProto, int channelId) {
		eagerPushPeers.remove(msg.getSender());
		lazyPushPeers.add(msg.getSender());
	}

	private void uponPlumtreeGraftMessage(PlumtreeGraftMessage msg, Host from, short sourceProto, int channelId) {
		eagerPushPeers.add(msg.getSender());
		lazyPushPeers.remove(msg.getSender());
		if (received.contains(msg.getMid()))
			sendMessage(msg, msg.getSender());
	}

	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}


	/*------------------------------- Procedures -------------------------------------- */

	private void eagerPushGossip(PlumtreeGossipMessage msg) {

		eagerPushPeers.forEach(host -> {
			if (!host.equals(myself)) {
				logger.trace("Sent {} to {}", msg, myself);
				sendMessage(msg, host);
			}
		});
	}

	private void lazyPushGossip(PlumtreeGossipMessage msg) {

		lazyIHaveMessage.addMessageId(msg.getMid());
		simpleAnnouncementPolicy();

	}

	private void simpleAnnouncementPolicy() {
		// TODO: cópia da message e dos peers para nao poderem ser modificados?

		lazyPushPeers.forEach(h -> {
			sendMessage(lazyIHaveMessage, myself);
		});
		lazyIHaveMessage.clearMessageIds();
	}

	private void lessSimpleAnnouncementPolicy() {
		// TODO: cópia da message e dos peers para nao poderem ser modificados?

		lazyPushPeers.forEach(h -> {
			sendMessage(lazyIHaveMessage, myself);
		});

		/*
		 * TODO: Juntar lista de uuids na ihavemessage e criar um timer que
		 * periodicamente envia todas as que existem mas, se houverem
		 * mudanças no lazyPushPeers (add ou remove) tem de se enviar
		 * tudo antes de adicionar ou remover. Criar um boolean que diz
		 * se naquele periodo do timer já enviou entretanto sem ser por
		 * culpa do timer, se sim, não envia, senão envia.
		 */
	}

	private void optimization(PlumtreeGossipMessage msg) {
		missing.forEach(iHaveMsg -> {
			if(iHaveMsg.getMessageIds().contains(msg.getMid())) {
				int r = iHaveMsg.getRound();
				int round = msg.getRound()-1;
		        if(r < round && (round - r) >= THRESHOLD) {
		        	sendMessage(new PlumtreeGraftMessage(UUID.randomUUID(), myself, r, null), iHaveMsg.getSender());
					sendMessage(new PlumtreePruneMessage(UUID.randomUUID(), myself), msg.getSender());
		        }
			}
		});
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

			missing.forEach(msg -> {
				if (msg.getSender().equals(h)) {
					missing.remove(msg);
				}
			});

			logger.info("Neighbour down: " + h);
		}
	}


	/*--------------------------------- Timers ---------------------------------------- */

	private void uponMissingMessageTimer(MissingMessageTimer missingMessageTimer, long timerId) {
		missingMessageTimers.remove(missingMessageTimer.getMessageId());

		long timer = setupTimer(new MissingMessageTimer(missingMessageTimer.getMessageId()), SHORTER_MISSING_TIMEOUT);
		missingMessageTimers.put(missingMessageTimer.getMessageId(), timer);
		
		for(PlumtreeIHaveMessage msg : missing) {
			if (msg.getMessageIds().contains(missingMessageTimer.getMessageId())) {
				eagerPushPeers.add(msg.getSender());
				lazyPushPeers.remove(msg.getSender());
				// TODO: no pseudo código aqui dizia para remover o primeiro announcement??
				sendMessage(new PlumtreeGraftMessage(UUID.randomUUID(), myself, msg.getRound(),
						missingMessageTimer.getMessageId()), msg.getSender());
				break;
			}
		}
	}
}
