package protocols.membership.hyparview;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import babel.generic.ProtoMessage;
import channel.tcp.TCPChannel;
import channel.tcp.events.ChannelMetrics;
import channel.tcp.events.InConnectionDown;
import channel.tcp.events.InConnectionUp;
import channel.tcp.events.OutConnectionDown;
import channel.tcp.events.OutConnectionFailed;
import channel.tcp.events.OutConnectionUp;
import network.data.Host;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import protocols.membership.full.timers.InfoTimer;
import protocols.membership.hyparview.messages.ForwardJoin;
import protocols.membership.hyparview.messages.ForwardJoinReply;
import protocols.membership.hyparview.messages.JoinRequest;
import protocols.membership.hyparview.messages.NeighborRequest;
import protocols.membership.hyparview.messages.NeighborRequestReply;
import protocols.membership.hyparview.messages.Shuffle;
import protocols.membership.hyparview.messages.ShuffleRequestReply;
import protocols.membership.hyparview.timers.NeighbourTimer;
import protocols.membership.hyparview.timers.ShuffleTimer;

public class HyParView extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(HyParView.class);

	// Protocol information, to register in babel
	public final static short PROTOCOL_ID = 420;
	public final static String PROTOCOL_NAME = "HyParView";

	private final Host self; // My own address/port
	private final Set<Host> activeView; // Small active view of size fanout+1
	private final Set<Host> passiveView; // Large passive view of size greater than log(n)
	private final Set<Host> pendingActive, pendingNeighbour, testNeighbours;

	// Protocol parameters
	private final int fanout, passiveViewSize;
	private final int ARWL, PRWL, ka, kb;
	private final int shuffleTimeout, neighbourTimeout;
	
	private final Random rnd;

	private final int channelId;

	public HyParView(Properties props, Host self) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);

		this.self = self;
		this.activeView = new HashSet<>();
		this.pendingActive = new HashSet<>();
		this.pendingNeighbour = new HashSet<>();
		this.passiveView = new HashSet<>();	
		this.testNeighbours = new HashSet<>();


		this.rnd = new Random();

		// Get some configurations from properties file
		this.fanout = Integer.parseInt(props.getProperty("fanout", "3"));
		this.ARWL = Integer.parseInt(props.getProperty("arwl", "4"));
		this.PRWL = Integer.parseInt(props.getProperty("prwl", "2"));
		this.ka = Integer.parseInt(props.getProperty("ka", "3"));
		this.kb = Integer.parseInt(props.getProperty("kb", "6"));
		this.passiveViewSize = Integer.parseInt(props.getProperty("passive_view_size", "8"));
		this.shuffleTimeout = Integer.parseInt(props.getProperty("shuffle_timeout", "7000"));
		this.neighbourTimeout = Integer.parseInt(props.getProperty("neighbour_timeout", "7000"));

		String cMetricsInterval = props.getProperty("channel_metrics_interval", "10000"); // 10 seconds

		// Create a properties object to setup channel-specific properties. See the
		// channel description for more details.
		Properties channelProps = new Properties();
		channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); // The address to bind to
		channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port")); // The port to bind to
		channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); // The interval to receive channel
																						// metrics
		channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); // Heartbeats interval for established
																				// connections
		channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); // Time passed without heartbeats until
																				// closing a connection
		channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); // TCP connect timeout
		channelId = createChannel(TCPChannel.NAME, channelProps); // Create the channel with the given properties

		/*---------------------- Register Message Serializers ---------------------- */
		registerMessageSerializer(channelId, JoinRequest.MSG_ID, JoinRequest.serializer);
		registerMessageSerializer(channelId, ForwardJoin.MSG_ID, ForwardJoin.serializer);
		registerMessageSerializer(channelId, ForwardJoinReply.MSG_ID, ForwardJoinReply.serializer);
		registerMessageSerializer(channelId, NeighborRequest.MSG_ID, NeighborRequest.serializer);
		registerMessageSerializer(channelId, NeighborRequestReply.MSG_ID, NeighborRequestReply.serializer);
		registerMessageSerializer(channelId, Shuffle.MSG_ID, Shuffle.serializer);
		registerMessageSerializer(channelId, ShuffleRequestReply.MSG_ID, ShuffleRequestReply.serializer);

		/*---------------------- Register Message Handlers -------------------------- */
		registerMessageHandler(channelId, JoinRequest.MSG_ID, this::uponJoinRequest, this::uponMsgFail);
		registerMessageHandler(channelId, ForwardJoin.MSG_ID, this::uponForwardJoin, this::uponMsgFail);
		registerMessageHandler(channelId, ForwardJoinReply.MSG_ID, this::uponForwardJoinReply, this::uponMsgFail);
		registerMessageHandler(channelId, NeighborRequest.MSG_ID, this::uponNeighborRequest, this::uponMsgFail);
		registerMessageHandler(channelId, NeighborRequestReply.MSG_ID, this::uponNeighborRequestReply,
				this::uponMsgFail);
		registerMessageHandler(channelId, Shuffle.MSG_ID, this::uponShuffle, this::uponMsgFail);
		registerMessageHandler(channelId, ShuffleRequestReply.MSG_ID, this::uponShuffleReply, this::uponMsgFail);

		/*--------------------- Register Timer Handlers ----------------------------- */
		registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);
		registerTimerHandler(ShuffleTimer.TIMER_ID, this::uponShuffleTimer);
		registerTimerHandler(NeighbourTimer.TIMER_ID, this::uponNeighbourTimer);

		/*-------------------- Register Channel Events ------------------------------- */
		registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
		registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
		registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
		registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
		registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
		registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
	}

	@Override
	public void init(Properties props) {

		// Inform the dissemination protocol about the channel we created in the constructor
		triggerNotification(new ChannelCreated(channelId));

		// If there is a contact node, attempt to establish connection
		if (props.containsKey("contact")) {
			try {
				logger.debug("Trying to reach contact node");
				String contact = props.getProperty("contact");
				String[] hostElems = contact.split(":");
				Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
				addToActiveView(contactHost);
				openConnection(contactHost);
				logger.debug("sent Join Request");
				sendMessage(new JoinRequest(), contactHost);
			} catch (Exception e) {
				logger.error("Invalid contact on configuration: '" + props.getProperty("contacts"));
				e.printStackTrace();
				System.exit(-1);
			}
		}

		setupPeriodicTimer(new ShuffleTimer(), shuffleTimeout, shuffleTimeout);
		setupPeriodicTimer(new NeighbourTimer(), neighbourTimeout, neighbourTimeout);

		// Setup the timer to display protocol information (also registered handler previously)
		int pMetricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "10000"));
		if (pMetricsInterval > 0)
			setupPeriodicTimer(new InfoTimer(), pMetricsInterval, pMetricsInterval);
	}

	
	/*--------------------------------- Messages ---------------------------------------- */

	private void uponJoinRequest(JoinRequest jrq, Host from, short sourceProto, int channelId) {
		logger.debug("Received Join Request {} from {}", jrq, from);
		addToActiveView(from);

		for (Host h : activeView) {
			if (!h.equals(from)) {
				logger.info("Sent Forward Join to " + h.toString());
				sendMessage(new ForwardJoin(from, ARWL), h);
			}
		}

	}

	private void uponForwardJoin(ForwardJoin fwdjoin, Host from, short sourceProto, int channelId) {
		int ttl = fwdjoin.getTTL();
		Host node = fwdjoin.getnewNode();
		logger.debug("Received Forward Join from " + from.toString() + " With new node " + node.toString());
		
		
			if (ttl == 0 || (activeView.size() <= 1 && !self.equals(node))) {
				addToActiveView(node);
				if(!self.equals(node))
					sendMessage(new ForwardJoinReply(self),node);
			} else {
	
				if (ttl == PRWL) {
					addToPassiveView(node);
				}
	
				ttl--;
				Host nodeToSend = getRandom(getRandomSubsetExcluding(activeView, activeView.size() - 1, from));
				if(nodeToSend != null)
					sendMessage(new ForwardJoin(node, ttl), nodeToSend);
				else
					sendMessage(new ForwardJoin(node, ttl), from);

			}
	}
	
	
	private void uponForwardJoinReply(ForwardJoinReply fwdJoinReply , Host from , short sourceProto , int channelId) {
		logger.debug("Received Forward Join Reply from " + from.toString());
		addToActiveView(from);
	}

	private void uponNeighborRequest(NeighborRequest nbreq, Host from, short sourceProto, int channelId) {
		boolean highPrio = nbreq.getPriority();
		Host node = nbreq.getNode();
		logger.debug("NeighborRequest with prio " + highPrio + " from node " + from.toString() + " with req node "
				+ node.toString());

		if (highPrio) {
			addToActiveView(node);
			logger.debug("sent NeighborRequest Reply accepted to " + node.toString());
			sendMessage(new NeighborRequestReply(true), node);
		} else {
			if (activeView.size() < fanout + 1) {
				addToActiveView(node);
				logger.debug("sent NeighborRequest Reply accepted to " + node.toString());
				sendMessage(new NeighborRequestReply(true), node);
			} else {
				pendingNeighbour.add(node);
				openConnection(node);
				logger.debug("sent NeighborRequest Reply rejected to " + node.toString());
				sendMessage(new NeighborRequestReply(false), node);
			}
		}
	}

	private void uponNeighborRequestReply(NeighborRequestReply nbreqReply, Host from, short sourceProto,
			int channelId) {
		boolean accepted = nbreqReply.getAccepted();
		logger.debug("NeighborRequestReply accepted: " + accepted + " from node " + from.toString());

		if (accepted) {
			passiveView.remove(from);
			addToActiveView(from);
		} else {
			logger.debug("Closing connection " + from.toString());
			closeConnection(from);
			Host p = getRandom(passiveView);
			openConnection(p);
		}
	}

	private void uponShuffle(Shuffle shuffle, Host from, short sourceProto, int channelId) {
		logger.debug("Receveid Shuffle Operation from " + from.toString());
		int ttl = shuffle.getTTL();
		shuffle.setTTL(ttl- 1);

		if (ttl > 0 && activeView.size() > 1) {
			Host node = getRandom(getRandomSubsetExcluding(activeView, activeView.size(), from));
			logger.debug("sent Shuffle to " + node.toString());
			sendMessage(shuffle, node);
		} else {
			openConnection(from);
			Set<Host> kActiveView = getRandomSubsetExcluding(activeView, ka, null);
			Set<Host> kPassiveView = getRandomSubsetExcluding(passiveView, kb, null);
			kActiveView.addAll(kPassiveView);
			ShuffleRequestReply srp = new ShuffleRequestReply(self, kActiveView,
					shuffle.getNodes());
			logger.debug("sent Shuffle reply to " + from.toString());
			sendMessage(srp, from);

			for (Host node : shuffle.getNodes()) {
				if (passiveView.size() > passiveViewSize) {
					if (!(node.equals(self) || passiveView.contains(node) || activeView.contains(node))) {

						boolean done = false;
						for (Host pNode : kPassiveView) {
							if (passiveView.contains(pNode)) {
								passiveView.remove(pNode);
								addToPassiveView(node);
								done = true;
							}
						}

						if (!done) {
							addToPassiveView(node);
							done = true;
						}
					}
				} else
					addToPassiveView(node);
			}
		}
	}

	private void uponShuffleReply(ShuffleRequestReply shuffleReply, Host from, short sourceProto, int channelId) {
		logger.debug("Receveid ShuffleReply Operation from " + from.toString());
		Set<Host> nodesReceived = shuffleReply.getNodesReceived();

		for (Host node : shuffleReply.getNodes()) {
			if (passiveView.size() > passiveViewSize) {
				if (!(node.equals(self) || passiveView.contains(node) || activeView.contains(node))) {

					boolean done = false;
					for (Host pNode : nodesReceived) {
						if (passiveView.contains(pNode)) {
							passiveView.remove(pNode);
							addToPassiveView(node);
							done = true;
						}
					}

					if (!done) {
						addToPassiveView(node);
						done = true;
					}
				}
			} else
				addToPassiveView(node);
		}
	}

	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	
	/*--------------------------------- Timers ---------------------------------------- */

	private void uponShuffleTimer(ShuffleTimer timer, long timerId) {
		if (activeView.size() > 0) {
			Host node = getRandom(activeView);
			Set<Host> kActiveView = getRandomSubsetExcluding(activeView, ka, node);
			Set<Host> kPassiveView = getRandomSubsetExcluding(passiveView, kb, node);
			logger.debug("TIMER : Sent shuffle message  to " + node.toString());
			kActiveView.addAll(kPassiveView);
			sendMessage(new Shuffle(self, kActiveView, ARWL), node);
		}
	}
	
	
	private void uponNeighbourTimer(NeighbourTimer timer , long timerId) {
		testNeighbours.clear();
		if ( activeView.size() < (fanout + 1)) {
			Host p = getRandom(passiveView);
			if (p != null) {
				pendingNeighbour.add(p);
				openConnection(p);
			}
		}
	}

	/*--------------------------------- Utils ---------------------------------------- */

	private void addToActiveView(Host node) {
		logger.debug("Trying to Add To Active View " + node.toString());

		if (!node.equals(self) && !activeView.contains(node)) {
			pendingActive.add(node);
			logger.debug("Added to pending view node " + node.toString());
			openConnection(node);

		}
	}

	private void addToPassiveView(Host node) {
		logger.debug("Trying to Add To Passive View " + node.toString());

		if (!node.equals(self) && !activeView.contains(node) && !passiveView.contains(node)) {

			if (passiveView.size() >= passiveViewSize) {
				Host n = getRandom(passiveView);
				passiveView.remove(n);
			}

			passiveView.add(node);
		}
	}

	// Gets a random element from the set of peers
	private Host getRandom(Set<Host> hostSet) {
		if (hostSet.size() > 0) {
			int idx = rnd.nextInt(hostSet.size());
			int i = 0;
			for (Host h : hostSet) {
				if (i == idx)
					return h;
				i++;
			}
			return null;
		}
		return null;
	}

	// Gets a random subset from the set of hosts
	private static Set<Host> getRandomSubsetExcluding(Set<Host> hostSet, int sampleSize, Host exclude) {
		List<Host> list = new LinkedList<>(hostSet);
		list.remove(exclude);
		Collections.shuffle(list);
		return new HashSet<>(list.subList(0, Math.min(sampleSize, list.size())));
	}

	
	/* --------------------------------- TCPChannel Events ---------------------------- */

	// If a connection is successfully established, this event is triggered.
	private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
		Host peer = event.getNode();
		logger.debug("Connection to {} is up", peer);

		// Check if connection is pending
		if (pendingActive.contains(peer)) {
			if (activeView.size() >= (fanout + 1)) {
				Host random = getRandom(activeView);
				activeView.remove(random);
				logger.debug("removing from active view, disconnecting" + random.toString());
				closeConnection(random);
				logger.debug("Removed from active view node " + random.toString());
				triggerNotification(new NeighbourDown(random));
			}
			logger.debug("Im adding to active View " + peer.toString());
			pendingActive.remove(peer);
			activeView.add(peer);
			triggerNotification(new NeighbourUp(peer));

		}
		

		if (passiveView.contains(peer) && !pendingNeighbour.contains(peer) && !activeView.contains(peer)) {
			if(testNeighbours.contains(peer)) {
				boolean prio = activeView.size() == 0;
				logger.debug("Sent neighborRequest with prio: " + prio + " to " + peer);
				sendMessage(new NeighborRequest(self, prio), peer);
				testNeighbours.add(peer);
			}
			else {
				logger.debug("Closing connection if pending neighbor or passiveView " + peer.toString());
				closeConnection(peer);
			}
			
		}

	}

	// If an established connection is disconnected
	private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
		Host peer = event.getNode();
		logger.debug("Connection to {} is down cause {}", peer, event.getCause());

		if (activeView.contains(peer)) {
			activeView.remove(peer);
			addToPassiveView(peer);
			triggerNotification(new NeighbourDown(event.getNode()));
			Host p = getRandom(passiveView);
			if (p != null) {
				pendingNeighbour.add(p);
				openConnection(p);
			}

		}
	}

	// If a connection fails to be established, this event is triggered. In this
	// protocol, we simply remove from the pending set.
	// Note that this event is only triggered while attempting a connection, not after connection.
	private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
		logger.debug("Connection to {} failed cause: {}", event.getNode(), event.getCause());
		Host peer = event.getNode();
		if(pendingActive.contains(peer))
			pendingActive.remove(peer);
		
		if(pendingNeighbour.contains(peer))
			pendingNeighbour.remove(peer);
		
		if (passiveView.contains(peer)) {
			passiveView.remove(peer);
			Host p = getRandom(passiveView);
			if (p != null) {
				pendingNeighbour.add(p);
				openConnection(p);
			}
		}
	}

	// If someone established a connection to me
	private void uponInConnectionUp(InConnectionUp event, int channelId) {
		logger.debug("Connection from {} is up", event.getNode());
	}

	// A connection someone established to me is disconnected.
	private void uponInConnectionDown(InConnectionDown event, int channelId) {
		logger.debug("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
		Host peer = event.getNode();

		if(pendingActive.contains(peer))
			pendingActive.remove(peer);
		
		
		if (activeView.contains(peer)) {
			activeView.remove(peer);
			logger.debug("Someone disconnect from me " + peer.toString());
			closeConnection(peer);
			addToPassiveView(peer);
			triggerNotification(new NeighbourDown(event.getNode()));
			Host p = getRandom(passiveView);
			if (p != null) {
				pendingNeighbour.add(p);
				openConnection(p);
			}
		}
	}

	
	/* --------------------------------- Metrics ---------------------------- */

	// If we setup the InfoTimer in the constructor, this event will be triggered
	// periodically.
	// We are simply printing some information to present during runtime.
	private void uponInfoTime(InfoTimer timer, long timerId) {
		StringBuilder sb = new StringBuilder("Membership Metrics:\n");
		sb.append("Active View: ").append(activeView).append("\n");
		sb.append("Passive View: ").append(passiveView).append("\n");
		// getMetrics returns an object with the number of events of each type processed
		// by this protocol.
		// It may or may not be useful to you, but at least you know it exists.
		sb.append(getMetrics());
		logger.info(sb);
	}

	// If we passed a value >0 in the METRICS_INTERVAL_KEY property of the channel,
	// this event will be triggered
	// periodically by the channel. This is NOT a protocol timer, but a channel
	// event.
	// Again, we are just showing some of the information you can get from the
	// channel, and use how you see fit.
	// "getInConnections" and "getOutConnections" returns the currently established
	// connection to/from me.
	// "getOldInConnections" and "getOldOutConnections" returns connections that
	// have already been closed.
	private void uponChannelMetrics(ChannelMetrics event, int channelId) {
		StringBuilder sb = new StringBuilder("Channel Metrics:\n");
		sb.append("Active View :\n");
		activeView.forEach(c -> sb.append(String.format("\tNode : %s \n", c.toString())));

		sb.append("Passive View:\n");
		passiveView.forEach(c -> sb.append(String.format("\tNode : %s \n", c.toString())));

		sb.setLength(sb.length() - 1);
		logger.info(sb);
	}
}
