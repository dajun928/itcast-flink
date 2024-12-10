package cn.itcast.flink.transformation;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink流计算中DataStream转换算子：max或maxBy\min或minBy
 *
 * @author xuanyu
 */
public class TransformationMaxMinDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStream<Tuple3<String, String, Integer>> inputDataStream = env.fromElements(
            Tuple3.of("上海", "浦东新区", 777),
            Tuple3.of("上海", "闵行区", 999),
            Tuple3.of("上海", "杨浦区", 666),
            Tuple3.of("北京", "东城区", 567),
            Tuple3.of("北京", "西城区", 987),
            Tuple3.of("上海", "静安区", 888),
            Tuple3.of("北京", "海淀区", 9999)
        );

        // 3. 数据转换-transformation
        // todo: max 最大值，只关心指定字段最大值，其他字段不关心
        DataStream<Tuple3<String, String, Integer>> maxDataStream = inputDataStream
            .keyBy(tuple -> tuple.f0)
            .max(2);
        //maxDataStream.printToErr("max>") ;

        // todo: maxBy 最大值，不仅关心指定字段最大值，而且还关系其他字段
        DataStream<Tuple3<String, String, Integer>> maxByDataStream = inputDataStream
            .keyBy(tuple -> tuple.f0)
            .maxBy(2);
        maxByDataStream.print("maxBy>");

        // 4. 数据终端-sink

        // 5. 触发执行-execute
        env.execute("TransformationMaxMinDemo");
    }

}  