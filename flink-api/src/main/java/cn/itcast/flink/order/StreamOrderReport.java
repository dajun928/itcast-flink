package cn.itcast.flink.order;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 仿双十一实时大屏： 从Kafka Topic实时消费交易订单数据，销售总额统计，保存到Redis内存数据库。
 *
 * @author xuanyu
 */
public class StreamOrderReport {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(3);

        // 2. 数据源-source
        // 2-1. Kafka消费者属性配置
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1.itcast.cn:9092,node2.itcast.cn:9092,node3.itcast.cn:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "gid-order-1");
        // 2-2. 创建Consumer消费对象
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
            "order-topic", new SimpleStringSchema(), props
        );
        kafkaConsumer.setStartFromLatest();
        // 2-3. 添加数据源
        DataStreamSource<String> kafkaStream = env.addSource(kafkaConsumer);

        // 3. 数据转换-transformation
        /*
            业务数据
                # orderId,userId,orderMoney,orderTime
                2022051506560053115929,u_12839,50.0,1652568960531
            3-1. 将数据提取字段值，封装到元组中
                ("全国", 50.0)
            3-2. 按照["全国"]分组，对组内数据求和累加
                keyBy算子 + sum 算子
         */
        // 3-1. 将数据提取字段值，封装到元组中
        SingleOutputStreamOperator<Tuple2<String, Double>> mapStream = kafkaStream.map(
            new MapFunction<String, Tuple2<String, Double>>() {
                @Override
                public Tuple2<String, Double> map(String value) throws Exception {
                    // 分割字符串
                    String[] array = value.split(",");
                    // 获取销售额
                    double orderMoney = Double.parseDouble(array[2]);
                    // 构建二元组，并且返回输出
                    return Tuple2.of("全国", orderMoney);
                }
            }
        );

        // 3-2. 按照["全国"]分组，对组内数据求和累加
        SingleOutputStreamOperator<Tuple2<String, Double>> reportStream = mapStream
            .keyBy(tuple -> tuple.f0)
            .sum(1);
        // reportStream.printToErr();

        // 4. 数据接收器-sink todo: key -> “全国",  value -> totalMoney
        // 4-1-a. 连接Redis单机服务
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
            .setHost("node1.itcast.cn").setPort(6379)
            .setMaxTotal(8).setMaxIdle(8).setMinIdle(3)
            .setDatabase(0)
            .build();
        // 4-1-b. 将数据映射为命令
        RedisMapper<Tuple2<String, Double>> redisMapper = new RedisMapper<Tuple2<String, Double>>() {
            // (全国,50.0)  ->  SET “全国" "50.0"
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.SET);
            }

            @Override
            public String getKeyFromData(Tuple2<String, Double> data) {
                return data.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, Double> data) {
                return data.f1.toString();
            }
        };
        // 4-1-c. 传递参数，创建实例
        RedisSink<Tuple2<String, Double>> redisSink = new RedisSink<>(
            jedisPoolConfig, redisMapper
        );
        // 4-2. 添加接收器
        reportStream.addSink(redisSink).setParallelism(1);

        // 5. 触发执行-execute
        env.execute("StreamOrderReport");
    }

}
