package cn.itcast.flink.connector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Flink从Kafka 队列消费数据，指定topic名称和其他一些参数
 *
 * @author xuanyu
 */
public class ConnectorFlinkKafkaConsumerDemo {

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
        // 2-2. 添加数据源
        DataStreamSource<String> kafkaStream = env.addSource(kafkaConsumer);

        // 3. 数据转换-transformation

        // 4. 数据终端-sink
        kafkaStream.print();

        // 5. 触发执行-execute
        env.execute("ConnectorFlinkKafkaConsumerDemo");
    }

}  