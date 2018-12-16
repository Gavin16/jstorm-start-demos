package zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * zookeeper 节点创建和删除 的监听
 */
public class SimpleWatcherDemo {

    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {

        ZooKeeper zk = new ZooKeeper("192.168.0.105:2181", 6000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("state : "+event.getState());
            }
        });

        // sequential 节点连续创建三个
        zk.create("/testRoot",null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        zk.create("/testRoot",null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        zk.create("/testRoot",null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        Stat stat = zk.exists("/testRoot", new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getPath()+"|"+watchedEvent.getType().name());

                try {
                    zk.exists("/testRoot",this);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }

            }
        });
        // 节点存在则打印节点长度
        if(null != stat){
            System.out.println("DataLength : " + stat.getDataLength());
        }
//        Watcher watcher = event -> {System.out.println(event.getPath()+"|"+event.getType().name());};
//        zk.exists("/testRoot",watcher);
        Thread.sleep(100*1000);
        zk.close();
    }
}
