package cn.itcast.flink.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink 流计算中DataStream的flatMap算子可以替代filter算子和map算子功能
 *
 * @author xuanyu
 */
public class TransformationFlatMapTest {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);

        // 3. 数据转换-transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleStream = inputStream.flatMap(
            new FlatMapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                    // value -> 输入数据流中每条数据, todo: value 字段值不能为空
                    if (value.trim().length() > 0) {
                        String[] words = value.split("\\s+");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                }
            }
        );

        DataStream<Tuple2<String, Integer>> resultStream = tupleStream
            .keyBy(tuple -> tuple.f0)
            .sum(1);

        // 4. 数据终端-sink
        resultStream.print();

        // 5. 触发执行-execute
        env.execute("TransformationFlatMapTest");
    }

}  