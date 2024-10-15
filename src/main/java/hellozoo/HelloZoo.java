package hellozoo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class HelloZoo {

    private static final Logger LOG = LoggerFactory.getLogger(HelloZoo.class);

    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {

        ZooKeeper zookeeper = new ZooKeeper("localhost:2181", 15000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                LOG.info("Got the event for node " + event.getPath());
                LOG.info("Event type = " + event.getType());
                LOG.info("********************");
            }
        });

        // CREATE
        String newNodePath = "/newnode_2";
        if (zookeeper.exists(newNodePath, false) == null) {
            zookeeper.create(newNodePath, "Ngo Ba Thien".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOG.info("Node created: " + newNodePath);
        } else {
            LOG.info("Node " + newNodePath + " already exists.");
        }

        // Chỉ cập nhật nếu node đã tồn tại
        zookeeper.setData(newNodePath, "This is new data".getBytes(), -1);
        LOG.info("Updated node data for: " + newNodePath);

        // READ
        Stat stat = new Stat();
        byte[] data = zookeeper.getData(newNodePath, true, stat);
        LOG.info("Data read from node: " + new String(data));
        LOG.info("Version: " + stat.getVersion());

        List<String> children = zookeeper.getChildren(newNodePath, true);
        children.forEach(child -> LOG.info("Child found: " + child));

//         DELETE
//         Chỉ xóa nếu node tồn tại
//        if (zookeeper.exists(newNodePath, false) != null) {
//            zookeeper.delete(newNodePath, -1);
//            LOG.info("Node deleted: " + newNodePath);
//        }

        Thread.sleep(100_000);
    }
}
