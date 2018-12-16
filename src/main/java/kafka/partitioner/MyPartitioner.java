package kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 自定义生产者消息发送 分区
 */
public class MyPartitioner implements Partitioner {

    private Random random;

    /**
     * 如果key 中包含"my" 则默认返回编号最大的partition
     * @param topic
     * @param keyObj
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    @Override
    public int partition(String topic, Object keyObj, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        String key  = (String) keyObj;
        List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);

        int partitionCount = partitionInfos.size();
        int myPartition = partitionCount - 1;
        boolean condition =  (key == null || key.isEmpty() || !key.contains("my"));

        return condition ? random.nextInt(partitionCount - 1): myPartition;
    }

    @Override
    public void close() {
        // 关闭资源
    }

    @Override
    public void configure(Map<String, ?> map) {
        this.random = new Random();
    }
}
