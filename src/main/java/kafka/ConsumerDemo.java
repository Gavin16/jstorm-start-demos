package kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    // 订阅主题
    private static final String topicName = "topic2";
    // consumer group
    private static final String groupId = "test-group";

    public static void main(String[] args) {

        Properties props = new Properties();

        // server, group.id, key.deserializer, value.deserializer四个参数无默认值，必须配置
        // 注意这里 服务器地址配置的 主机名:端口号, 需要在研发环境修改hosts 文件
        props.put("bootstrap.servers","ubuntu1:9092");
        props.put("group.id",groupId);
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // 是否允许consumer 位移自动提交
        props.put("enable.auto.commit","true");
        // consumer 位移自动提交时间间隔
        props.put("auto.commit.interval.ms","1000");
        // auto.offset.reset 设置为 earliest 指定从最早的位移开始消费，但是如果之前有位移提交,则启动时从位移提交处开始消费
        // auto.offset.reset 通常还可以设置为 latest, 设置为latest 指的从最新处位移开始消费
        props.put("auto.offset.reset","earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topicName));

        try {
            while(true){
                ConsumerRecords<String,String> records = consumer.poll(2000);
                for(ConsumerRecord<String,String> record : records){
                    System.out.printf("订阅消息 offset=%d,key=%s,value=%s%n",record.offset(),record.key(),record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
