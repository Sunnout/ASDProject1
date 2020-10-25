package protocols.broadcast.plumtree;

import java.io.IOException;
import java.util.ArrayList;
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
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;

public class PlumtreeBroadcast extends GenericProtocol {
	private static final Logger logger = LogManager.getLogger(PlumtreeBroadcast.class);

	// Protocol information, to register in babel
	public static final String PROTOCOL_NAME = "Plumtree";
	public static final short PROTOCOL_ID = 600;

	private final Host myself; // My own address/port
	private final Set<Host> eagerPushPeers; // Neighbours with which to use eager push gossip
	private final Set<Host> lazyPushPeers; // Neighbours with which to use lazy push gossip
	private final ArrayList<PlumtreeIHaveMessage> lazyMessageQueue; // List msg announcements to be sent
	private final ArrayList<Host> lazyHostQueue; // List host to send announcements to

	private final Set<PlumtreeIHaveMessage> missing; // Set of missing messages
	private final Set<UUID> received; // Set of received messages

	// We can only start sending messages after the membership protocol informed us
	// that the channel is ready
	private boolean channelReady;

	public PlumtreeBroadcast(Properties properties, Host myself) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.myself = myself;
		eagerPushPeers = new HashSet<>(); // TODO: initially contains f (fanout) random peers. o que fazer quando inicia
		lazyPushPeers = new HashSet<>(); // initially empty
		lazyMessageQueue = new ArrayList<>(); // initially empty
		lazyHostQueue = new ArrayList<>(); // initially empty
		missing = new HashSet<>(); // initially empty
		received = new HashSet<>(); // initially empty
		channelReady = false;

		/*--------------------- Register Request Handlers ----------------------------- */
		registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

		/*--------------------- Register Notification Handlers ----------------------------- */
		subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
		subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
		subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
	}

	@Override
	public void init(Properties props) {
		// TODO: inicializo aqui as coisas ou no construtor??
		// Nothing to do here, we just wait for event from the membership or the
		// application

		// tenho de esperar até ter f neighbours para iniciar o protocolo?
	}

	// Upon receiving the channelId from the membership, register our own callbacks
	// and serializers
	private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
		int cId = notification.getChannelId();
		// Allows this protocol to receive events from this channel.
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
		// Now we can start sending messages
		channelReady = true;
	}

	/*--------------------------------- Requests ---------------------------------------- */

	private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
		if (!channelReady)
			return;

		// Send msg via eager push gossip
		PlumtreeGossipMessage gossipMsg = new PlumtreeGossipMessage(request.getMsgId(), request.getSender(), 0,
				sourceProto, request.getMsg());
		eagerPushGossip(gossipMsg, myself);

		// Add IHAVE msg to announcements
		// TODO: como criar Id da mensagem nova?
		PlumtreeIHaveMessage iHaveMsg = new PlumtreeIHaveMessage(request.getMsgId(), request.getSender(), 0,
				sourceProto, request.getMsgId());
		lazyPushGossip(iHaveMsg, myself);

		// Deliver msg
		triggerNotification(new DeliverNotification(gossipMsg.getMid(), gossipMsg.getSender(), gossipMsg.getContent()));

		// Add msgId to set of received msgIds
		received.add(gossipMsg.getMid());

		// uponPlumtreeMessage(msg, myself, getProtoId(), -1);
	}

	/*--------------------------------- Messages ---------------------------------------- */

	private void uponPlumtreeGossipMessage(PlumtreeGossipMessage msg, Host from, short sourceProto, int channelId) {
		logger.trace("Received {} from {}", msg, from);

		if (received.add(msg.getMid())) {
			// Deliver msg
			triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));

			missing.forEach(missMsg -> {
				if (missMsg.getContent().equals(msg.getMid())) {
					// TODO: Cancel timer(msg.getMid())
					// TODO: missing.remove(missMsg); não temos de tirar a message do missing?
				}
			});

			// Increment round and send msg via eager push gossip
			msg.incrementRound();
			eagerPushGossip(msg, myself);

			// TODO: como criar Id da mensagem nova?
			PlumtreeIHaveMessage iHaveMsg = new PlumtreeIHaveMessage(msg.getMid(), msg.getSender(), msg.getRound(),
					sourceProto, msg.getMid());
			lazyPushGossip(iHaveMsg, myself);

			eagerPushPeers.add(msg.getSender());
			lazyPushPeers.remove(msg.getSender());
			optimization();

		} else {
			eagerPushPeers.remove(msg.getSender());
			lazyPushPeers.add(msg.getSender());
			// TODO: Send PRUNE msg
		}
	}

	private void uponPlumtreeIHaveMessage(PlumtreeIHaveMessage msg, Host from, short sourceProto, int channelId) {
		if (!received.contains(msg.getContent())) {
			// if there is no Timer for msg.getContent() then, setup timer
			missing.add(msg);
		}
	}

	private void uponPlumtreePruneMessage(PlumtreePruneMessage msg, Host from, short sourceProto, int channelId) {
		eagerPushPeers.remove(msg.getSender());
		lazyPushPeers.add(msg.getSender());
	}

	private void uponPlumtreeGraftMessage(PlumtreeGraftMessage msg, Host from, short sourceProto, int channelId) {
		eagerPushPeers.add(msg.getSender());
		lazyPushPeers.remove(msg.getSender());
		if (!received.add(msg.getMid()))
			sendMessage(msg, msg.getSender());
	}

	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	private void eagerPushGossip(PlumtreeGossipMessage msg, Host from) {
		// TODO: mandar ronda na mensagem ou como parâmetro separado?

		eagerPushPeers.forEach(host -> {
			if (!host.equals(from)) {
				logger.trace("Sent {} to {}", msg, host);
				sendMessage(msg, host);
			}
		});
	}

	private void lazyPushGossip(PlumtreeIHaveMessage msg, Host from) {
		// TODO: mandar ronda na mensagem ou como parâmetro separado?

		lazyPushPeers.forEach(host -> {
			if (!host.equals(from)) {
				lazyMessageQueue.add(msg);
				lazyHostQueue.add(host);
			}
		});

		dispatchAnnouncements();
	}

	private void dispatchAnnouncements() {
		PlumtreeIHaveMessage[] announcements = (PlumtreeIHaveMessage[]) lazyMessageQueue.toArray();
		Host[] targets = (Host[]) lazyHostQueue.toArray();

		for (int i = 0; i < announcements.length; i++) {
			// TODO: create compact message here to send only one announcement???
			// hostId1:msgId1;hostId2;msgId2...
			lazyMessageQueue.remove(0);
			lazyHostQueue.remove(0);
		}
	}

	private void optimization() {
		// TODO
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
}
