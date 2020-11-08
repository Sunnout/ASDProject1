package protocols.membership.hyparview.messages;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

public class ShuffleRequestReply extends ProtoMessage {

	public final static short MSG_ID = 691;
    private final Set<Host> node_activeView, node_passiveView, nodesReceived;	
	private final Host node;
	
	public ShuffleRequestReply(Host node, Set<Host> node_activeView, Set<Host> node_passiveView, Set<Host> nodesReceived) {
		super(MSG_ID);
		this.node = node;
		this.node_activeView = node_activeView;
		this.node_passiveView = node_passiveView;
		this.nodesReceived = nodesReceived;
	}
	
	
	
	public Set<Host> getNode_activeView() {
		return node_activeView;
	}

	public Set<Host> getNode_passiveView() {
		return node_passiveView;
	}

	public Set<Host> getNodesReceived() {
		return nodesReceived;
	}
	
	public Host getNode() {
		return node;
	
	}
	
	
	
	 public static ISerializer<ShuffleRequestReply> serializer = new ISerializer<ShuffleRequestReply>() {
	        @Override
	        public void serialize(ShuffleRequestReply shuffleMessage, ByteBuf out) throws IOException {
	        	Set<Host> active = shuffleMessage.getNode_activeView();
	        	Set<Host> passive = shuffleMessage.getNode_passiveView();
	        	Set<Host> nodesReceived = shuffleMessage.getNodesReceived();
	        	Host node = shuffleMessage.getNode();
	        	
	            out.writeInt(active.size());
	            for (Host h : active)
	                Host.serializer.serialize(h, out);
	            
	            out.writeInt(passive.size());
	            for (Host h : passive)
	                Host.serializer.serialize(h, out);
	            
	            out.writeInt(nodesReceived.size());
	            for (Host h : nodesReceived)
	                Host.serializer.serialize(h, out);
	            
                Host.serializer.serialize(node, out);

	            
	        }

	        @Override
	        public ShuffleRequestReply deserialize(ByteBuf in) throws IOException {
	            int size = in.readInt();
	            Set<Host> active_subset = new HashSet<>(size, 1);
	            for (int i = 0; i < size; i++)
	                active_subset.add(Host.serializer.deserialize(in));
	            
	            size = in.readInt();
	            Set<Host> passive_subset = new HashSet<>(size, 1);
	            for (int i = 0; i < size; i++)
	                passive_subset.add(Host.serializer.deserialize(in));
	            
	            
	            size = in.readInt();
	            Set<Host> nodesReceived = new HashSet<>(size, 1);
	            for (int i = 0; i < size; i++)
	            	nodesReceived.add(Host.serializer.deserialize(in));
	            
	            Host node = Host.serializer.deserialize(in);
	            
	            
	            
	            return new ShuffleRequestReply(node,active_subset,passive_subset,nodesReceived);
	        }
	    };
	

}



