package kafka;

import kafka.domain.Computer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.RetriableException;

import java.util.Properties;

/**
 * 参考  Apache Kafka 实战
 */
public class ProducerDemo {

    static private final String TOPIC = "topic1";
    static private final String BROKER_LIST = "192.168.0.105:9092";


    public static void main(String[] args) throws Exception {
//        Producer<String, String> producer = initProducer();
//        sendOne(producer, TOPIC);

        // kafka 实战 连接kafka服务器最小配置; bootstrap.servers, key.serializer, value.serializer 为必须设置的3个参数
        // 连接服务器时  kafka server.properties 文件中配置的是主机名 ubuntu ，报无法解析异常 解决办法：
        // 修改 C:\Windows\System32\drivers\etc\hosts 文件, 在里面添加IP到主机的映射关系
        Properties props = new Properties();
        props.put("bootstrap.servers",BROKER_LIST);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        // acks 指定了 partition 中leader broker 在接收到producer 的消息后必须写入的 副本数; acks 通常可能的取值有 0,1,all(-1)
        //    acks = 0  则表示producer 完全不理睬 leader broker 的处理结果, 在发送完一条消息后不等待leader broker 的返回结果就开始下一次发送
        //          由于不等待发送结果得  通常这种方式可以有效提高producer的吞吐率；同时如果发送失败了 producer是不知道的
        //    acks = 1 表示设置 leader broker 在接收到producer 的消息并将消息写入本地日志，就可以发送响应结果给producer
        //          而无需等待其它ISR中的副本，这样只要leader broker 一直存活,kafka 就能够保证这一条消息不丢失
        //    acks = -1(all)  表示 leader broker 在接收到producer 的消息之后 不经需要将记录写入本地日志，同时还要将记录写入ISR中所有的其它成员
        //          才会向 producer发送响应结果; 这样只要ISR中存在一个存活的副本，消息记录就不会丢失; 当副本数较多的 producer的吞吐量将变得较低
        props.put("acks","-1");
        // 由于网络抖动或者leader选举等原因, producer 发送的消息可能会失败，可以在properties 参数中设置producer的重发次数
        //  retries = 0 表示不做重发; producer 认为的发送失败 有可能并不是真正的发送失败，而是在broker提交后发送响应给producer producer由于某种原因
        //  没有成功接收到, 这将导致producer 向broker 发送重复的消息，因此retries > 0 时需要consumer在消费时对消息采取去重处理
        props.put("retries","3");
        //  producer 将发往同一分区的多条消息封装进一个batch 中，当batch 满了的时候，producer 会发送batch中的所有消息
        //  可以通过 配置batch.size 来设置 batch 容量的大小； batch 过大占用过多内存，batch 过小
        props.put("batch.size","323840");
        // producer 在向broker发送消息时如果是等到 batch已经满了再发送 有可能因为 producer的吞吐量比较小，batch需要等较长时间才能满
        // 这个时候如果等待就会话较长时间， linger.ms 参数就是用来设置这种消息发送延时的行为的，linger 设置的较大会让生产者发送消息的延时变大
        // linger 设置的较小会让生产者发送消息的吞吐量变小， 吞吐量和延时之间存在矛盾 需要权衡设置
        props.put("linger.ms",10);
        // buffer.memory 指定producer 用户缓冲消息的内存大小,
        props.put("buffer.memory",33554432);
        // 设置单条消息最大大小
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,1024*1024);
        // 设置请求超时时间，producer 向 broker发送消息后 等待时长，如果超过这个时长 producer就会认为响应超时了
        props.put("max.block.ms",6000);
//        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,6000);
//        ProducerConfig config = new ProducerConfig(props);
        // 指定使用 topic 下的哪一个分区
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "kafka.partitioner.MyPartitioner");



        Producer<String,String> producer = new KafkaProducer<>(props);


        // 使用 producer 发送后的回调函数 做后续处理
        producer.send(new ProducerRecord<>(TOPIC, "test001","18218179305"), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(null == e){
                    System.out.println("kafka 生产消息发送成功");
                }else{
                    if(e instanceof RetriableException){
                        System.out.println("发生可重试异常");
                    }else{
                        System.out.println("发生不可重试异常");
                    }
                }
            }
        });

        // 测试 对topic设定partition
        ProducerRecord<String,String> record1 = new ProducerRecord<>(TOPIC,"non-test","partition setting");
        producer.send(record1).get();

        // 发送序列化 实体类
        sendSerilizableDto(props);

        producer.close();

    }

    /**
     * 发送序列化数据
     * @param props
     */
    public static void sendSerilizableDto(Properties props){
        // 增加自定义的 序列化实现类
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"kafka.serializer.ComputerSerializer");
        // 定义新的 producer
        Producer<String,Object> producer = new KafkaProducer<>(props);
        // 需要发送的java 实体类
        Computer computer = new Computer("desktop","Intel-i7","12G","256G",6789);
        // 待发送的记录
        ProducerRecord<String,Object> serilizableDto = new ProducerRecord<>(TOPIC,"computer",computer);
        //
        producer.send(serilizableDto);

        producer.close();
    }

}
