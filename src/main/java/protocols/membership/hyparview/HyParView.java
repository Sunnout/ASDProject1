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
import protocols.membership.hyparview.messages.JoinRequest;
import protocols.membership.hyparview.messages.NeighborRequest;
import protocols.membership.hyparview.messages.Shuffle;

public class HyParView extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(HyParView.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 420;
    public final static String PROTOCOL_NAME = "HyParView";

    private final Host self;     //My own address/port
    private final Set<Host> activeView; // small active view of size fanout +1 
    private final Set<Host> passiveView; //large passive view of size greater than log(n)

    private final int fanout; //param: maximum size of sample;
    private final int ARWL , PRWL; 

    private final Random rnd;

    private final int channelId; //Id of the created channel

    public HyParView(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.self = self;
        this.activeView = new HashSet<>();
        this.passiveView = new HashSet<>();

        this.rnd = new Random();

        //Get some configurations from the Properties object
        this.fanout = 3 ; /// TODO get from props
        this.ARWL = 3;
        this.PRWL = 1;
        
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
        registerMessageSerializer(channelId, JoinRequest.MSG_ID, JoinRequest.serializer);
        registerMessageSerializer(channelId, ForwardJoin.MSG_ID, ForwardJoin.serializer);    
        registerMessageSerializer(channelId, NeighborRequest.MSG_ID, NeighborRequest.serializer);
        registerMessageSerializer(channelId, Shuffle.MSG_ID, Shuffle.serializer);




        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, JoinRequest.MSG_ID, this::uponJoinRequest, this::uponMsgFail);
        registerMessageHandler(channelId, ForwardJoin.MSG_ID, this::uponForwardJoin, this::uponMsgFail);
        registerMessageHandler(channelId, NeighborRequest.MSG_ID, this::uponNeighborRequest, this::uponMsgFail);



        /*--------------------- Register Timer Handlers ----------------------------- */
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
                logger.info("Trying to reach contact node");
                String contact = props.getProperty("contact");
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                //TODO add to active View or only add when forward join comes ?
                openConnection(contactHost);
                sendMessage(new JoinRequest(), contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + props.getProperty("contacts"));
                e.printStackTrace();
                System.exit(-1);
            }
        }
        

        //Setup the timer to display protocol information (also registered handler previously)
        int pMetricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "10000"));
        if (pMetricsInterval > 0)
            setupPeriodicTimer(new InfoTimer(), pMetricsInterval, pMetricsInterval);
    }

    /*--------------------------------- Messages ---------------------------------------- */  
 
    private void uponJoinRequest( JoinRequest jrq , Host from , short sourceProto , int channelId) {
        logger.info("Received Join Request {} from {}",jrq, from);
        
        for(Host h : activeView) {
        	sendMessage(new ForwardJoin(from,ARWL),h);
        }
       
        addToActiveView(from);	
    }
    
    private void uponForwardJoin(ForwardJoin fwdjoin, Host from ,short sourceProto , int channelId) {
    	int ttl = fwdjoin.getTTL();
    	Host node = fwdjoin.getnewNode();
    	
    	if (ttl == 0 || activeView.size() == 1) {
    		addToActiveView(node);
    	}
    	else if( ttl == PRWL) {
    		passiveView.add(node);
    	}
    	else {
    		ttl --;
    		Host nodeToSend = getRandom(getRandomSubsetExcluding(activeView,activeView.size()-1,from));
    		sendMessage(new ForwardJoin(from,ttl),nodeToSend);
    	}
    }
    
    
    
    private void uponNeighborRequest(NeighborRequest nbreq, Host from ,short sourceProto , int channelId) {
    	boolean highPrio = nbreq.getPriority();
    	Host node = nbreq.getNode();
    	if(highPrio) {
    		addToActiveView(node);
    		//TODO Accept the request
    	}
    	else {
    		if(activeView.size() < fanout +1) {
          		activeView.add(node);
            	openConnection(node);
    		}
    		else {
    			//TODO refuse the request
    		}
    	}
    	
    	
    }
    
    

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
                             Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /*--------------------------------- Timers ---------------------------------------- */
 

    /*--------------------------------- Utils ---------------------------------------- */

    
    private void addToActiveView(Host node) {
    	logger.info("Adding " + node.toString());
        //protocol. Alternatively, we could do smarter things like retrying the connection X times.
    	
    	  if(activeView.size() < (fanout + 1)) {
          	
          		activeView.add(node);
          }
          else {
	        	Host random = getRandom(activeView);
	          	activeView.remove(random);
	          	closeConnection(random);
	          	
	          	activeView.add(node);	          	
          }
    	  
    	openConnection(node);
    	
    	logger.info("Active View size = " + activeView.size());

    }
    
    //Gets a random element from the set of peers
    private Host getRandom(Set<Host> hostSet) {
        int idx = rnd.nextInt(hostSet.size());
        int i = 0;
        for (Host h : hostSet) {
            if (i == idx)
                return h;
            i++;
        }
        return null;
    }
    
    
    //Gets a random subset from the set of hosts
    private static Set<Host> getRandomSubsetExcluding(Set<Host> hostSet, int sampleSize, Host exclude) {
        List<Host> list = new LinkedList<>(hostSet);
        list.remove(exclude);
        Collections.shuffle(list);
        return new HashSet<>(list.subList(0, Math.min(sampleSize, list.size())));
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */

    //If a connection is successfully established, this event is triggered. In this protocol, we want to add the
    //respective peer to the membership, and inform the Dissemination protocol via a notification.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();

        logger.debug("Connection to {} is up", peer);
        // TODO Is this if really necessary ? ????
        if(passiveView.contains(peer)) {
            passiveView.remove(peer);
        }
        
        // should never enter this if , please code it nicely
        if(!activeView.contains(peer)) {
        	logger.error("Got connection without a host in my active View , check your code");
        }
        
       boolean prio = activeView.size() == 0; 
       
       sendMessage(new NeighborRequest(self,prio), peer);
        
    }

    //If an established connection is disconnected
    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} is down cause {}", peer, event.getCause());
        //if the node that failed belongs to the active view, find a q node from its passive view and try to connect to it
        if(activeView.contains(peer)) {
        	activeView.remove(peer);
        	Host p = getRandom(passiveView);
        	openConnection(p);

        }
        //triggerNotification(new NeighbourDown(event.getNode())); ??
    }

    //If a connection fails to be established, this event is triggered. In this protocol, we simply remove from the
    //pending set. Note that this event is only triggered while attempting a connection, not after connection.

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.debug("Connection to {} failed cause: {}", event.getNode(), event.getCause());
        Host peer = event.getNode();
        
        if(passiveView.contains(peer)) {
        	Host p = getRandom(passiveView);
        	openConnection(p);
        }
     
 
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
        sb.append("Active View: ").append(activeView).append("\n");
        sb.append("Passive View: ").append(passiveView).append("\n");
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

