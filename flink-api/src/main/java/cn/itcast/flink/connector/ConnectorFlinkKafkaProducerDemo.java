package cn.itcast.flink.connector;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 将DataStream数据流中数据保存到Kafka topic队列中，使用FlinkKafkaProducer类完成
 *
 * @author xuanyu
 */
public class ConnectorFlinkKafkaProducerDemo {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Order {
        private String id;
        private Integer userId;
        private Double money;
        private Long orderTime;
    }

    /**
     * 自定义数据源：每隔1秒产生1条交易订单数据
     */
    private static class OrderSource extends RichParallelSourceFunction<Order> {
        // 定义标识变量，表示是否产生数据
        private boolean isRunning = true;

        // 模拟产生交易订单数据
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                // 构建交易订单数据
                // 构建交易订单数据
                Order order = new Order(
                    UUID.randomUUID().toString(), //
                    random.nextInt(10) + 1, //
                    (double) random.nextInt(100),//
                    System.currentTimeMillis()
                );
                // 将数据输出
                ctx.collect(order);

                // 每隔1秒产生1条数据，线程休眠
                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * 创建子类，实现接口KafkaSerializationSchema，对数据进行序列化操作
     */
    private static class KafkaStringSchema implements KafkaSerializationSchema<String> {

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
            // element -> 表示 数据流中每条数据，也就是写入topic队列中数据，需要将其转换为byte[]字节数组
            return new ProducerRecord<byte[], byte[]>("flink-topic", element.getBytes());
        }

    }


    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 2. 数据源-source
        DataStreamSource<Order> orderDataStream = env.addSource(new OrderSource());

        // 3. 数据转换-transformation
        // todo: 将订单数据Order对象，转换为JSON字符串，存储到Kafka topic 队列
        SingleOutputStreamOperator<String> jsonDataStream = orderDataStream.map(
            new MapFunction<Order, String>() {
                @Override
                public String map(Order order) throws Exception {
                    // 使用阿里巴巴：fastJson库，转换类对象为JSON字符串
                    return JSON.toJSONString(order);
                }
            }
        );
        // jsonDataStream.printToErr() ;

        // 4. 数据终端-sink
        // 4-1. 创建FlinkKafkaProducer对象
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1.itcast.cn:9092,node2.itcast.cn:9092,node3.itcast.cn:9092");
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
            "flink-topic", new KafkaStringSchema(), props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        // 4-2. 添加接收器
        jsonDataStream.addSink(kafkaProducer);

        // 5. 触发执行-execute
        env.execute("ConnectorFlinkKafkaProducerDemo");
    }

}  