package cn.itcast.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Flink 流计算数据源：基于集合的Source，分别为可变参数、集合和自动生成序列，todo： 属于有界流 -> 数据处理完成，job结束
 *
 * @author xuanyu
 */
public class StreamSourceCollectionDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        // 方式一： 可变参数
        DataStreamSource<String> dataStream01 = env.fromElements("spark", "flink", "mapreduce");
        dataStream01.print();

        // 方式二：集合对象
        DataStreamSource<String> dataStream02 = env.fromCollection(Arrays.asList("spark", "flink", "mapreduce"));
        // 打印数据到控制台，以错误输出方式，字体为红色
        dataStream02.printToErr();

        // 方式三：自动生成序列数字，封装在数据流DataStream
        DataStreamSource<Long> dataStream03 = env.fromSequence(1, 10);
        dataStream03.print("sequence");

        // 3. 数据转换-transformation

        // 4. 数据终端-sink

        // 5. 触发执行-execute
        env.execute("StreamSourceCollectionDemo");
    }

}  