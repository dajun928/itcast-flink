package cn.itcast.flink.connector;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink从Kafka队列消费数据，使用新的类：KafkaSource，创建对象时使用builder建造者模式创建
 *
 * @author xuanyu
 */
public class ConnectorKafkaSourceDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 2. 数据源-source
        // 2-1. 创建KafkaSource实例对象
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("node1.itcast.cn:9092,node2.itcast.cn:9092,node3.itcast.cn:9092")
            .setTopics("flink-topic")
            .setGroupId("gid-flink-2")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperty("partition.discovery.interval.ms", "5000")
            .build();
        // 2-2. 从数据源获取数据
        DataStreamSource<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 3. 数据转换-transformation

        // 4. 数据终端-sink
        kafkaStream.printToErr();

        // 5. 触发执行-execute
        env.execute("ConnectorKafkaSourceDemo");
    }

}  