package protocols.broadcast.eagerpush;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
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
import protocols.broadcast.eagerpush.messages.EagerPushMessage;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;

public class EagerPushBroadcast extends GenericProtocol {
	private static final Logger logger = LogManager.getLogger(EagerPushBroadcast.class);

	// Protocol information, to register in babel
	public static final String PROTOCOL_NAME = "EagerPush";
	public static final short PROTOCOL_ID = 500;

	private final Host myself; // My own address/port
	private final Set<Host> neighbours; // My known neighbours (a.k.a peers the membership protocol told me about)
	private final Set<UUID> received; // Set of received messages (since we do not want to deliver the same msg twice)
	private final int fanout; // Number of neighbours to send gossip message to

	private boolean channelReady;

	public EagerPushBroadcast(Properties properties, Host myself) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.myself = myself;
		neighbours = new HashSet<>();
		received = new HashSet<>();
		channelReady = false;
		fanout = (int)Math.ceil(Math.log(Integer.parseInt(properties.getProperty("node_magnitude"))));

		/*--------------------- Register Request Handlers ----------------------------- */
		registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

		/*--------------------- Register Notification Handlers ------------------------- */
		subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
		subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
		subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
	}

	@Override
	public void init(Properties props) {
		// Nothing to do here, we just wait for event from the membership or the
		// application
	}

	// Upon receiving the channelId from the membership, register callbacks and serializers
	private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
		int cId = notification.getChannelId();
		registerSharedChannel(cId);
		/*---------------------- Register Message Serializers ---------------------- */
		registerMessageSerializer(cId, EagerPushMessage.MSG_ID, EagerPushMessage.serializer);
		/*---------------------- Register Message Handlers -------------------------- */
		try {
			registerMessageHandler(cId, EagerPushMessage.MSG_ID, this::uponEagerPushMessage, this::uponMsgFail);
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

		EagerPushMessage msg = new EagerPushMessage(request.getMsgId(), request.getSender(), sourceProto,
				request.getMsg());

		// Call the same handler as when receiving a new EagerPushMessage
		uponEagerPushMessage(msg, myself, getProtoId(), -1);
	}

	/*--------------------------------- Messages ---------------------------------------- */
	
	private void uponEagerPushMessage(EagerPushMessage msg, Host from, short sourceProto, int channelId) {
		logger.info("Received {} from {}", msg, from);
		// If we already received it once, do nothing
		if (received.add(msg.getMid())) {
			// Deliver the message to the application
			triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));

			Set<Host> sample = getRandomSubsetExcluding(neighbours, fanout, from);
			// Simply send the message to a subset of size fanout of known neighbours
			sample.forEach(host -> {
				logger.info("Sent {} to {}", msg, host);
				sendMessage(msg, host);
			});
		}
	}

	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		// If a message fails to be sent, for whatever reason, log the message and the
		// reason
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	
	/*----------------------------------- Procedures ----------------------------------------- */

	private static Set<Host> getRandomSubsetExcluding(Set<Host> hostSet, int sampleSize, Host exclude) {
		List<Host> list = new LinkedList<>(hostSet);
		list.remove(exclude);
		Collections.shuffle(list);
		return new HashSet<>(list.subList(0, Math.min(sampleSize, list.size())));
	}


	/*--------------------------------- Notifications ---------------------------------------- */

	private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
		for (Host h : notification.getNeighbours()) {
			neighbours.add(h);
			logger.info("New neighbour: " + h);
		}
	}

	private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
		for (Host h : notification.getNeighbours()) {
			neighbours.remove(h);
			logger.info("Neighbour down: " + h);
		}
	}
}
