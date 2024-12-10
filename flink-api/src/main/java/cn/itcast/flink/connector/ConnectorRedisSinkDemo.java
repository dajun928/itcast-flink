package cn.itcast.flink.connector;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 将词频统计结果保存到Redis内存数据库，使用RedisSink接收器完成（todo: 可以自己实现SinkFunction接口）。
 *
 * @author xuanyu
 */
public class ConnectorRedisSinkDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<Tuple2<String, String>> inputDataStream = env.fromElements(
            Tuple2.of("flink", "888"), Tuple2.of("spark", "999"), Tuple2.of("hive", "1200")
        );

        // 3. 数据转换-transformation

        // 4. 数据终端-sink
        /*
            ("flink", "888")
            ("spark", "999")
            ----------------------------------------------------------
            todo: 将数据流中每条数据存储到Redis数据库中，考虑使用数据类型（value）： 使用hash类型存储
                key -> flink:word:count
                value -> hash
            将流中每条数据转换为对应操作命令
                ("flink", "888")  ->  HSET "flink:word:count" "flink" "888"
                ("spark", "999")  ->  HSET "flink:word:count" "spark" "999"
                                    |
                                RedisSink中接口RedisMapper
         */
        // 4-1. 创建RedisSink对象，传递参数
        // 4-1-a. 连接Redis单机服务
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
            .setHost("node1.itcast.cn").setPort(6379)
            .setMaxTotal(8).setMaxIdle(8).setMinIdle(3)
            .setDatabase(0)
            .build();
        // 4-1-b. 将数据映射为命令
        RedisMapper<Tuple2<String, String>> redisSinkMapper = new RedisMapper<Tuple2<String, String>>() {
            // ("flink", "888")  ->  HSET "flink:word:count" "flink" "888"

            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "flink:word:count");
            }

            @Override
            public String getKeyFromData(Tuple2<String, String> data) {
                return data.f0;
            }

            @Override
            public String getValueFromData(Tuple2<String, String> data) {
                return data.f1;
            }
        };
        // 4-1-c. 传递参数，创建实例
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<Tuple2<String, String>>(
            jedisPoolConfig, redisSinkMapper
        );
        // 4-2. 添加接收器
        inputDataStream.addSink(redisSink);

        // 5. 触发执行-execute
        env.execute("ConnectorRedisSinkDemo");
    }

}  