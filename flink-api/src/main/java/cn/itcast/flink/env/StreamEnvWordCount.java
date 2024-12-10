package cn.itcast.flink.env;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
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
public class StreamEnvWordCount {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration configuration = new Configuration();
        configuration.setString("rest.port", "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // todo 本地模式运行程序job，创建本地运行环境即可，指定web ui 端口号
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment() ;

        //Configuration configuration = new Configuration() ;
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        // 2. 数据源-source
        DataStreamSource<String> inputDataStream = env.socketTextStream("node1.itcast.cn", 9999);

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

        // 5. 触发执行-execute
        env.execute("StreamWordCount");
    }

}
