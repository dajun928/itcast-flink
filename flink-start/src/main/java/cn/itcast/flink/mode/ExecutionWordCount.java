package cn.itcast.flink.mode;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 使用Flink计算引擎实现实时流计算：词频统计WordCount，从TCP Socket消费数据，结果打印控制台。
 * 1. 执行环境-env
 * 2. 数据源-source
 * 3. 数据转换-transformation
 * 4. 数据接收器-sink
 * 5. 触发执行-execute
 *
 * @author xuanyu
 */
public class ExecutionWordCount {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // todo 设置执行运行时模式 -> BATCH, 就是批处理
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 2. 数据源-source
        DataStreamSource<String> inputDataStream = env.readTextFile("datas/words.txt");

        // 3. 数据转换-transformation
        /*
             流中每条数据：  flink flink spark
                             |
                   词频统计，步骤与批处理完全一致
         */
        // 3-1. 分割单词
        SingleOutputStreamOperator<String> wordDataStream = inputDataStream.flatMap(
            new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String value, Collector<String> out) throws Exception {
                    String[] words = value.split("\\s+");
                    for (String word : words) {
                        out.collect(word);
                    }
                }
            }
        );

        // 3-2. 转换二元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDataStream = wordDataStream.map(
            new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String value) throws Exception {
                    return Tuple2.of(value, 1);
                }
            }
        );

        // 3-3. 按照单词分组并且组内求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = tupleDataStream
            .keyBy(0)
            .sum(1);

        // 4. 数据接收器-sink
        resultDataStream.print();
        /*
            1> (hive,4)
            2> (python,3)
            1> (spark,7)
            3> (hdfs,1)
            4> (hadoop,2)
            4> (mapreduce,2)
         */
        resultDataStream.writeAsText("datas/execution-result-wc");

        // 5. 触发执行-execute
        env.execute("ExecutionWordCount");
    }

}
