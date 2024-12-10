package cn.itcast.flink.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
public class TransformationReduceDemo {

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
        /*
                    |map()
            (flink, 1),    (spark, 1),  (flink, 1),  (flink, 1)
                    |keyBy(word)
            flink  ->    [(flink, 1),  (flink, 1) ,  (flink, 1)  ]
                                            |reduce 算子聚合

         */
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = tupleDataStream
            .keyBy(tuple -> tuple.f0)
            .reduce(
                new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tmp,
                                                          Tuple2<String, Integer> item) throws Exception {
                        /*
                            tmp:
                                表示组内数据聚合中间结果值，比如(flink, 10)
                                todo: 如果key中第1次出现数据，直接将数据赋值给tmp，无需计算
                            item:
                                表示组内数据，就是流中数据，比如(flink, 1)
                         */
                        System.out.println("tmp = " + tmp + ", item = " + item);
                        // a. 获取以前计算值
                        Integer historyValue = tmp.f1;
                        // b. 获取当前传递进来的数据至
                        Integer currentValue = item.f1;
                        // c. 计算最新值，直接累加求和
                        int latestValue = historyValue + currentValue;
                        // d. 返回结果
                        return Tuple2.of(tmp.f0, latestValue);
                    }
                }
            );

        // 4. 数据终端-sink
        resultDataStream.printToErr();

        // 5. 触发执行-execute
        env.execute("TransformationKeyByDemo");
    }

}  