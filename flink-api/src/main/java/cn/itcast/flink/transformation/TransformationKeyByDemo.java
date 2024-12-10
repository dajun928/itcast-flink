package cn.itcast.flink.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink中流计算DataStream转换算子：keyBy 和 sum 算子
 *
 * @author xuanyu
 */
public class TransformationKeyByDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<String> inputDataStream = env.socketTextStream("node1.itcast.cn", 9999);

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
        // 3-0. 过滤掉空字符串
        SingleOutputStreamOperator<String> lineDataStream = inputDataStream.filter(
            new FilterFunction<String>() {
                @Override
                public boolean filter(String value) throws Exception {
                    return value.trim().length() > 0;
                }
            }
        );

        // 3-1. 分割单词，使用flatMap算子
        SingleOutputStreamOperator<String> wordDataStream = lineDataStream.flatMap(
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

        // 3-2. 转换为二元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDataStream = wordDataStream.map(
            new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String value) throws Exception {
                    return Tuple2.of(value, 1);
                }
            }
        );

        // 3-3. 按照单词分组，并且组内求和
        /*  数据流中数据类型为元组时，指定下标索引
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = tupleDataStream
                .keyBy(0)
                .sum(1);
        */

        /*  数据流中数据类型为JavaBean对象时，指定属性名称
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = tupleDataStream
                .keyBy("f0")
                .sum(1);
        */

        /*  keyBy算子对流中数据分组时，指定方式，使用匿名内部类对象
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = tupleDataStream
            .keyBy(
                    new KeySelector<Tuple2<String, Integer>, String>() {
                        @Override
                        public String getKey(Tuple2<String, Integer> value) throws Exception {
                            // value 表示 数据流DataStream中每条数据
                            return value.f0;
                        }
                    }
            )
            .sum(1);
        */

        // 使用Java8中Lambda表达式书写代码
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = tupleDataStream
            .keyBy(tuple -> tuple.f0)
            .sum(1);

        // 4. 数据终端-sink
        resultDataStream.printToErr();

        // 5. 触发执行-execute
        env.execute("TransformationKeyByDemo");
    }

}  