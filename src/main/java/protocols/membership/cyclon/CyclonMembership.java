package protocols.membership.cyclon;

import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import babel.generic.ProtoMessage;
import channel.tcp.TCPChannel;
import channel.tcp.events.*;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import protocols.membership.cyclon.messages.SampleMessageReply;
import protocols.membership.cyclon.messages.SampleMessage;
import protocols.membership.cyclon.timers.InfoTimer;
import protocols.membership.cyclon.timers.SampleTimer;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class CyclonMembership extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(CyclonMembership.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 1000;
    public final static String PROTOCOL_NAME = "CyclonMembership";

    private final Host self;     //My own address/port
    private final Set<Host> membership; //Peers I am connected to
    private final Set<Host> pending; //Peers I am trying to connect to
    private final Map<Host, Integer> membersAge;

    private final int sampleTime; //param: timeout for samples
    private final int subsetSize; //param: maximum size of sample;

    private final int channelId; //Id of the created channel

    public CyclonMembership(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.self = self;
        this.membership = new HashSet<>();
        this.pending = new HashSet<>();
        this.membersAge = new HashMap<>();

        //Get some configurations from the Properties object
        this.subsetSize = Integer.parseInt(props.getProperty("sample_size", "6"));
        this.sampleTime = Integer.parseInt(props.getProperty("sample_time", "2000")); //2 seconds

        String cMetricsInterval = props.getProperty("channel_metrics_interval", "10000"); //10 seconds

        //Create a properties object to setup channel-specific properties. See the channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port")); //The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); //Create the channel with the given properties

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, SampleMessage.MSG_ID, SampleMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, SampleMessage.MSG_ID, this::uponSample, this::uponMsgFail);
        registerMessageHandler(channelId, SampleMessageReply.MSG_ID, this::uponSampleReply, this::uponMsgFail);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(SampleTimer.TIMER_ID, this::uponSampleTimer);
        registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);

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

        //Inform the dissemination protocol about the channel we created in the constructor
        triggerNotification(new ChannelCreated(channelId));

        //If there is a contact node, attempt to establish connection
        if (props.containsKey("contact")) {
            try {
                String contact = props.getProperty("contact");
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                //We add to the pending set until the connection is successful
                pending.add(contactHost);
                openConnection(contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + props.getProperty("contacts"));
                e.printStackTrace();
                System.exit(-1);
            }
        }

        //Setup the timer used to send samples (we registered its handler on the constructor)
        setupPeriodicTimer(new SampleTimer(), this.sampleTime, this.sampleTime);

        //Setup the timer to display protocol information (also registered handler previously)
        int pMetricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "10000"));
        if (pMetricsInterval > 0)
            setupPeriodicTimer(new InfoTimer(), pMetricsInterval, pMetricsInterval);
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponSample(SampleMessage msg, Host from, short sourceProto, int channelId) {
        //Received a sample from a peer. We add all the unknown peers to the "pending" map and attempt to establish
        //a connection. If the connection is successful, we add the peer to the membership (in the connectionUp callback)
        logger.debug("Received {} from {}", msg, from);

        for (Host h : msg.getSample())
            if (!h.equals(self) && !membership.contains(h) && !pending.contains(h)) {
                pending.add(h);
                //Every channel operation is asynchronous! The result is returned in the form of channel events
                openConnection(h);
            }

        sendSampleReply(from);
    }

    private void uponSampleReply(SampleMessageReply msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);

        for (Host h : msg.getSample())
            if (!h.equals(self) && !membership.contains(h) && !pending.contains(h)) {
                pending.add(h);
                //Every channel operation is asynchronous! The result is returned in the form of channel events
                openConnection(h);
            }
    }

    private void sendSampleReply(Host target){
        Set<Host> subset = getRandomSubsetExcluding(membership, subsetSize, target);
        subset.add(self);
        sendMessage(new SampleMessageReply(subset), target);

        //Do this now or save the set and wait for the sample reply?
        removeSampleFromNeighbours(subset);
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
                             Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /*--------------------------- Membership Management ------------------------------ */

    //Removes a set of neighbours from membership set
    private void removeSampleFromNeighbours(Set<Host> hostsToRemove){
        for(Host h: hostsToRemove){
            membership.remove(h);
            membersAge.remove(h);
            closeConnection(h);
        }
    }

    //Gets the oldest known host
    //Always returns non null because ageMembership() is called before the first time this method is called
    private Host getOldest(Set<Host> hostSet) {
        Host toReturn = null;
        int currentAge = 0;
        int age;

        for (Host h: hostSet) {
            age = membersAge.get(h);
            if (age > currentAge) {
                toReturn = h;
                currentAge = age;
            }
        }

        return toReturn;
    }

    //Increments the age of the whole known membership
    private void increaseAge(Set<Host> hostSet){
        for (Host h: hostSet)
            membersAge.put(h, membersAge.get(h)+1);
    }

    //Gets a random subset from the set of peers
    private static Set<Host> getRandomSubsetExcluding(Set<Host> hostSet, int sampleSize, Host exclude) {
        List<Host> list = new LinkedList<>(hostSet);
        list.remove(exclude);
        Collections.shuffle(list);
        return new HashSet<>(list.subList(0, Math.min(sampleSize, list.size())));
    }

    /*--------------------------------- Timers ---------------------------------------- */
    private void uponSampleTimer(SampleTimer timer, long timerId) {
        //When the SampleTimer is triggered, get a random peer in the membership and send a sample
        logger.debug("Sample Time: membership{}", membership);
        if (membership.size() > 0) {
            increaseAge(membership);
            Host target = getOldest(membership);
            sendSample(target);
            logger.debug("Sent SampleMessage {}", target);
        }
    }

    private void sendSample(Host target) {
        Set<Host> subset = getRandomSubsetExcluding(membership, subsetSize, target);
        subset.add(self);
        sendMessage(new SampleMessage(subset), target);

        //Do this now or save the set and wait for the sample reply?
        removeSampleFromNeighbours(subset);
    }
    /* --------------------------------- TCPChannel Events ---------------------------- */

    //If a connection is successfully established, this event is triggered. In this protocol, we want to add the
    //respective peer to the membership, and inform the Dissemination protocol via a notification.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} is up", peer);
        pending.remove(peer);
        if (membership.add(peer)) {
            membersAge.put(peer, 0);
            triggerNotification(new NeighbourUp(peer));
        }
    }

    //If an established connection is disconnected, remove the peer from the membership and inform the Dissemination
    //protocol. Alternatively, we could do smarter things like retrying the connection X times.
    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} is down cause {}", peer, event.getCause());
        membership.remove(peer);
        membersAge.remove(peer);
        triggerNotification(new NeighbourDown(peer));
    }

    //If a connection fails to be established, this event is triggered. In this protocol, we simply remove from the
    //pending set. Note that this event is only triggered while attempting a connection, not after connection.
    //Thus the peer will be in the pending set, and not in the membership (unless something is very wrong with our code)
    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.debug("Connection to {} failed cause: {}", event.getNode(), event.getCause());
        pending.remove(event.getNode());
    }

    //If someone established a connection to me, this event is triggered. In this protocol we do nothing with this event.
    //If we want to add the peer to the membership, we will establish our own outgoing connection.
    // (not the smartest protocol, but its simple)
    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    //A connection someone established to me is disconnected.
    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

    /* --------------------------------- Metrics ---------------------------- */

    //If we setup the InfoTimer in the constructor, this event will be triggered periodically.
    //We are simply printing some information to present during runtime.
    private void uponInfoTime(InfoTimer timer, long timerId) {
        StringBuilder sb = new StringBuilder("Membership Metrics:\n");
        sb.append("Membership: ").append(membership).append("\n");
        sb.append("PendingMembership: ").append(pending).append("\n");
        //getMetrics returns an object with the number of events of each type processed by this protocol.
        //It may or may not be useful to you, but at least you know it exists.
        sb.append(getMetrics());
        logger.info(sb);
    }

    //If we passed a value >0 in the METRICS_INTERVAL_KEY property of the channel, this event will be triggered
    //periodically by the channel. This is NOT a protocol timer, but a channel event.
    //Again, we are just showing some of the information you can get from the channel, and use how you see fit.
    //"getInConnections" and "getOutConnections" returns the currently established connection to/from me.
    //"getOldInConnections" and "getOldOutConnections" returns connections that have already been closed.
    private void uponChannelMetrics(ChannelMetrics event, int channelId) {
        StringBuilder sb = new StringBuilder("Channel Metrics:\n");
        sb.append("In channels:\n");
        event.getInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.append("Out channels:\n");
        event.getOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.setLength(sb.length() - 1);
        logger.info(sb);
    }
}
