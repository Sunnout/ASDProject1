package protocols.membership.hyparview.messages;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

public class Shuffle extends ProtoMessage {

    public final static short MSG_ID = 692;
    private final Set<Host> nodes;	
    private int ttl;
	private final Host node;
	
	public Shuffle(Host node, Set<Host> nodes, int ttl) {
		super(MSG_ID);
		this.node = node;
		this.nodes = nodes;
		this.ttl = ttl;
	}
	
	
	
	public Set<Host> getNodes() {
		return nodes;
	}

	public Host getNode() {
		return node;
	
	}
	
	public int getTTL() {
		return ttl;
	}
	
	public void setTTL(int ttl) {
		this.ttl = ttl;
	}
	
	 public static ISerializer<Shuffle> serializer = new ISerializer<Shuffle>() {
	        @Override
	        public void serialize(Shuffle shuffleMessage, ByteBuf out) throws IOException {
	        	Set<Host> active = shuffleMessage.getNodes();
	        	Host node = shuffleMessage.getNode();
	        	int ttl  = shuffleMessage.getTTL();
	        	
	            out.writeInt(active.size());
	            for (Host h : active)
	                Host.serializer.serialize(h, out);
	            
                Host.serializer.serialize(node, out);
                out.writeInt(ttl);

	            
	        }

	        @Override
	        public Shuffle deserialize(ByteBuf in) throws IOException {
	            int size = in.readInt();
	            Set<Host> active_subset = new HashSet<>(size, 1);
	            for (int i = 0; i < size; i++)
	                active_subset.add(Host.serializer.deserialize(in));
	            
	            
	            Host node = Host.serializer.deserialize(in);
	            int ttl = in.readInt();
	            
	            
	            
	            return new Shuffle(node,active_subset,ttl);
	        }
	    };
	

}
