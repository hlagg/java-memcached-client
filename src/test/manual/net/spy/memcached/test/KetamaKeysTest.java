package net.spy.memcached.test;
import net.spy.memcached.protocol.ascii.*;
import net.spy.memcached.protocol.*;
import net.spy.memcached.*;
import net.spy.memcached.ops.*;
import java.util.ArrayList;
import java.util.List;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class KetamaKeysTest {
    public KetamaKeysTest() {
    }

    public static KetamaNodeLocator getNodeLocator(String serverList) throws Exception {
        BlockingQueue<Operation> rq = new ArrayBlockingQueue<Operation>(1);
        BlockingQueue<Operation> wq = new ArrayBlockingQueue<Operation>(1);
        BlockingQueue<Operation> iq = new ArrayBlockingQueue<Operation>(1);
        int bufSize = 1000;

        List<MemcachedNode> nodes = new ArrayList<MemcachedNode>();
        String[] hostPortArray = serverList.split(",");

        for(String hostPort : hostPortArray) {
            String[] pair = hostPort.split(":");
            if (pair.length != 2) {
                System.out.println("bad server string: " + hostPort);
                System.exit(1);
            }
            String host = pair[0];
            int port = Integer.parseInt(pair[1]);
            nodes.add(new AsciiMemcachedNodeImpl(
                          new InetSocketAddress(host, port),
                          SocketChannel.open(), bufSize, rq, wq, iq));
        }
        KetamaNodeLocator nodeLocator = new KetamaNodeLocator(nodes, HashAlgorithm.KETAMA_HASH);
        return nodeLocator;
    }

    public static void main(String [] args) throws Exception {
        String serverList = "10.0.1.1:11211,10.0.1.2:11211,10.0.1.3:11211,10.0.1.4:11211,10.0.1.5:11211,10.0.1.6:11211,10.0.1.7:11211,10.0.1.8:11211,192.168.1.1:11211,192.168.100.1:11211";
        KetamaNodeLocator nodeLocator = getNodeLocator(serverList);
        
        for(int i = 0; i < 10000; i++) {
            String key = Integer.toString(i);
            String host = nodeLocator.getPrimary(key).getSocketAddress().toString();
            System.out.println("key " + key + " is on host " + host);
        }
    }
}
