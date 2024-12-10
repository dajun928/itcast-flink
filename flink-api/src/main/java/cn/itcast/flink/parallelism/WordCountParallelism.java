package cn.itcast.flink.parallelism;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
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
public class WordCountParallelism {

    /**
     * 当运行Flink程序时，执行此类的main方法，传递参数，获取对应host和port值
     * todo: WordCount --host node1.itcast.cn --port 9999
     *
     * @param args 应用程序运行时传递参数
     */
    public static void main(String[] args) throws Exception {

        // 构建参数解析工具类ParameterTool对象
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (parameterTool.getNumberOfParameters() != 2) {
            System.out.println("Usage: WordCount --host <host> --port <port> ....................");
            System.exit(-1);
        }
        // 获取传递参数值
        String hostname = parameterTool.get("host");
        int port = parameterTool.getInt("port", 9999);

        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // todo: 执行环境级别并行度
        env.setParallelism(2);

        // 2. 数据源-source
        DataStreamSource<String> inputDataStream = env.socketTextStream(hostname, port);

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

        // 4. 数据接收器-sink todo: 算子基本并行度设置，优先最高
        resultDataStream.print().setParallelism(1);

        // 5. 触发执行-execute
        env.execute("StreamWordCount");
    }

}