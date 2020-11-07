package protocols.membership.cyclon.components;

import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Connection {

    private final Host host;
    private final int age;
    public static ISerializer<Connection> serializer = new ISerializer<Connection>() {
        public void serialize(Connection con, ByteBuf out) throws IOException {
            Host.serializer.serialize(con.host, out);
            out.writeInt(con.age);
        }

        public Connection deserialize(ByteBuf in) throws IOException {
            Host host = Host.serializer.deserialize(in);
            int age = in.readInt();
            return new Connection(host, age);
        }
    };

    public Connection(Host host, int age){
        this.host = host;
        this.age = age;
    }

    public Host getHost() {
        return host;
    }

    public int getAge() {
        return age;
    }


}
