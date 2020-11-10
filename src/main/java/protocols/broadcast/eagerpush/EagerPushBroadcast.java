package protocols.broadcast.eagerpush;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import protocols.broadcast.plumtree.timers.ClearReceivedMessagesTimer;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;

public class EagerPushBroadcast extends GenericProtocol {
	private static final Logger logger = LogManager.getLogger(EagerPushBroadcast.class);

	// Protocol information, to register in babel
	public static final String PROTOCOL_NAME = "EagerPush";
	public static final short PROTOCOL_ID = 500;
	
	// TODO: Protocol parameters to put in config.properties
	public static final int CLEAR_RECEIVED_TIMEOUT = 5000; // Timeout to clear received messages

	private final Host myself; // My own address/port
	private final Set<Host> neighbours; // Set of known neighbours
	private final Set<UUID> received; // Set of received msgIds
	private final Map<UUID, Long> receivedTimes; // Map of <msgIds, receivedTimes>
	private final int fanout; // Number of neighbours to send gossip message to

	private boolean channelReady;

	public EagerPushBroadcast(Properties properties, Host myself) throws HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.myself = myself;
		neighbours = new HashSet<>();
		received = new HashSet<>();
		channelReady = false;
		receivedTimes = new HashMap<>();
		fanout = (int)Math.ceil(Math.log(Integer.parseInt(properties.getProperty("node_magnitude"))));

		/*--------------------- Register Request Handlers ----------------------------- */
		registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

		/*--------------------- Register Notification Handlers ------------------------- */
		subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
		subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
		subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);

		/*--------------------- Register Timer Handlers ------------------------- */
		registerTimerHandler(ClearReceivedMessagesTimer.TIMER_ID, this::uponClearReceivedMessagesTimer);
	}

	@Override
	public void init(Properties props) {
		setupPeriodicTimer(new ClearReceivedMessagesTimer(), CLEAR_RECEIVED_TIMEOUT, CLEAR_RECEIVED_TIMEOUT);
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
		UUID msgId = msg.getMid();
		long receivedTime = System.currentTimeMillis();
		
		// If we already received it once, do nothing
		if (received.add(msg.getMid())) {
			receivedTimes.put(msgId, receivedTime);
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
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	
	/*-------------------------------------- Timers ----------------------------------------- */

	private void uponClearReceivedMessagesTimer(ClearReceivedMessagesTimer clearReceivedMessagesTimer, long timerId) {
		logger.info("Clearing Messages");
		logger.info("Messages Before Cleaning: {}", received.size());
		Iterator<UUID> it = received.iterator();
		while (it.hasNext()) {
			UUID msgId = (UUID)it.next();
			if(System.currentTimeMillis() > receivedTimes.get(msgId) + CLEAR_RECEIVED_TIMEOUT) {
				logger.debug("Removed one");
				receivedTimes.remove(msgId);
				it.remove();
			}
		}
		logger.info("Messages After Cleaning: {}", received.size());
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
	
	
	/*----------------------------------- Procedures ----------------------------------------- */

	private static Set<Host> getRandomSubsetExcluding(Set<Host> hostSet, int sampleSize, Host exclude) {
		List<Host> list = new LinkedList<>(hostSet);
		list.remove(exclude);
		Collections.shuffle(list);
		return new HashSet<>(list.subList(0, Math.min(sampleSize, list.size())));
	}
}
