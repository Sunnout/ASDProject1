package protocols.broadcast.eagerpush;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
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
	private final int fanout; // Number of neighbours to gossip message to

	// We can only start sending messages after the membership protocol informed us
	// that the channel is ready
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

		/*--------------------- Register Notification Handlers ----------------------------- */
		subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
		subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
		subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
	}

	@Override
	public void init(Properties props) {
		// Nothing to do here, we just wait for event from the membership or the
		// application
	}

	// Upon receiving the channelId from the membership, register our own callbacks
	// and serializers
	private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
		int cId = notification.getChannelId();
		// Allows this protocol to receive events from this channel.
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
		// Now we can start sending messages
		channelReady = true;
	}

	/*--------------------------------- Requests ---------------------------------------- */
	private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
		if (!channelReady)
			return;

		// Create the message object.
		EagerPushMessage msg = new EagerPushMessage(request.getMsgId(), request.getSender(), sourceProto,
				request.getMsg());

		// Call the same handler as when receiving a new FloodMessage (since the logic
		// is the same)
		uponEagerPushMessage(msg, myself, getProtoId(), -1);
	}

	/*--------------------------------- Messages ---------------------------------------- */
	private void uponEagerPushMessage(EagerPushMessage msg, Host from, short sourceProto, int channelId) {
		logger.trace("Received {} from {}", msg, from);
		// If we already received it once, do nothing (or we would end up with a nasty
		// infinite loop)
		if (received.add(msg.getMid())) {
			// Deliver the message to the application (even if it came from it)
			triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));

			if (neighbours.size() > 0) {
				Set<Host> sample = getNeighboursSample(from);
				// Simply send the message to every known neighbour (who will then do the same)
				sample.forEach(host -> {
					logger.trace("Sent {} to {}", msg, host);
					sendMessage(msg, host);
				});
			}
		}
	}

	private Set<Host> getNeighboursSample(Host from) {
		int numberNeighbours = neighbours.size();
		Random rand = new Random();
		Set<Host> sample = new HashSet<>();
		Host[] hosts = new Host[numberNeighbours];
		neighbours.toArray(hosts);

		if (numberNeighbours > fanout) {
			while (sample.size() != fanout) {
				Host host = hosts[rand.nextInt(numberNeighbours)];

				if (!host.equals(from))
					sample.add(host);
			}
		} else
			// TODO: if someone only has less neighbours than fanout, needed?
			// TODO: remove from??
			return neighbours;

		return sample;
	}

	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		// If a message fails to be sent, for whatever reason, log the message and the
		// reason
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	/*--------------------------------- Notifications ---------------------------------------- */

	// When the membership protocol notifies of a new neighbour (or leaving one)
	// simply update my list of neighbours.
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
