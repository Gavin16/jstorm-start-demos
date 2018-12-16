package zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * leader selection
 */
public class LeaderSelectionDemo {

    public static void main(String[] args) throws InterruptedException {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,5);

        CuratorFramework curator = CuratorFrameworkFactory.newClient("192.168.0.106:2181",retryPolicy);

        LeaderSelector leaderSelector = new LeaderSelector(curator,"/leaderSelector",new CustomizedAdapter());

        leaderSelector.autoRequeue();
        curator.start();
        leaderSelector.start();

        Thread.sleep(1000*1000);

        leaderSelector.close();
        curator.close();
    }

    public static class CustomizedAdapter extends LeaderSelectorListenerAdapter{

        @Override
        public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
            System.out.println("Take the leadership.");
            Thread.sleep(3000);
        }
    }
}
