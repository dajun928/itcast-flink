package cn.itcast.flink.connector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Flink从Kafka 队列消费数据，指定topic名称和其他一些参数, todo: 可以指定消费起始偏移量位置
 *
 * @author xuanyu
 */
public class ConnectorFlinkKafkaConsumerOffsetDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // todo: topic队列中分区数为3，每个分区数据被1个消费者消费数据
        env.setParallelism(3);

        // 2. 数据源-source
        // 2-1. 创建FlinkKafkaConsumer实例对象
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1.itcast.cn:9092,node2.itcast.cn:9092,node3.itcast.cn:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "gid-flink-1");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
            "flink-topic", new SimpleStringSchema(), props
        );

        // todo 1. 最早偏移量消费
        //kafkaConsumer.setStartFromEarliest();

        // todo 2. 最新偏移量消费
        //kafkaConsumer.setStartFromLatest();

        // todo 3. 从消费组上次消费位置消费
        //kafkaConsumer.setStartFromGroupOffsets();

        // todo 4. 指定时间戳开始消费数据
        //kafkaConsumer.setStartFromTimestamp(1658558763627L) ;

        // todo 5. 指定具体分区偏移量位置开始消费数据
        Map<KafkaTopicPartition, Long> specificStartupOffsets = new HashMap<>();
        specificStartupOffsets.put(new KafkaTopicPartition("flink-topic", 0), 2L);
        specificStartupOffsets.put(new KafkaTopicPartition("flink-topic", 1), 1L);
        specificStartupOffsets.put(new KafkaTopicPartition("flink-topic", 2), 2L);
        kafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets);

        // 2-2. 添加数据源
        DataStreamSource<String> kafkaStream = env.addSource(kafkaConsumer);

        // 3. 数据转换-transformation

        // 4. 数据终端-sink
        kafkaStream.print();

        // 5. 触发执行-execute
        env.execute("ConnectorFlinkKafkaConsumerDemo");
    }

}  