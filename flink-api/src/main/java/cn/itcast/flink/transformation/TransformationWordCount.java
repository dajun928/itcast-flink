package cn.itcast.flink.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink中流计算DataStream转换算子：filter、map、flatMap、keyBy和sum算子，实现词频统计WordCount
 *
 * @author xuanyu
 */
public class TransformationWordCount {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);

        // 3. 数据转换-transformation
        /*
            spark flink flink flink spark
                        |flatMap
            spark,  flink,  flink,   flink,   spark
                        |map
            (spark, 1),   (flink, 1),   (flink, 1),   (flink, 1),   (spark, 1)
                        |keyBy(0)
             spark ->  [ (spark, 1),  (spark, 1) ] ,    flink ->  [(flink, 1),   (flink, 1),  (flink, 1)]
                        |sum(1): todo -> 分组后，对每个组内数据求和操作
             spark ->  2,   flink -> 3
         */
        // 3-1. 过滤掉空字符串
        DataStream<String> lineStream = inputStream.filter(line -> line.trim().length() > 0);

        // 3-2. 分割单词，使用flatMap算子
        DataStream<String> wordStream = lineStream.flatMap(
            new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String value, Collector<String> out) throws Exception {
                    String[] words = value.trim().split("\\s+");
                    for (String word : words) {
                        out.collect(word);
                    }
                }
            }
        );

        // 3-3. 转换为二元组
        DataStream<Tuple2<String, Integer>> tupleStream = wordStream.map(
            new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String value) throws Exception {
                    return Tuple2.of(value, 1);
                }
            }
        );

        // 3-4. 按照单词分组，并且组内求和
        DataStream<Tuple2<String, Integer>> resultStream = tupleStream
            .keyBy(tuple -> tuple.f0)
            .sum(1);

        // 4. 数据终端-sink
        resultStream.printToErr();

        // 5. 触发执行-execute
        env.execute("TransformationWordCount");
    }

}  