package cn.itcast.flink.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 使用Flink计算引擎实现数据处理：filter算子【高级-DataStream API】 和 process 算子【底层-State Processing API】
 *
 * @author xuanyu
 */
public class TransformationProcessDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);

        // 3. 数据转换-transformation
        // todo: filter 算子，过滤数据
        SingleOutputStreamOperator<String> filterStream = inputStream.filter(
            new FilterFunction<String>() {
                @Override
                public boolean filter(String value) throws Exception {
                    return value.trim().length() > 0;
                }
            }
        );
        filterStream.printToErr("filter>");

        // todo: process 算子，传递ProcessFunction函数接口实例对象，实现数据过滤功能
        SingleOutputStreamOperator<String> processStream = inputStream.process(
            new ProcessFunction<String, String>() {
                // value -> 表示 数据流中每条数据，  ctx -> Job 运行的上下文封装的对象，   out -> 收集器，将数据发送到下游
                @Override
                public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                    if (value.trim().length() > 0) {
                        out.collect(value);
                    }
                }
            }
        );
        processStream.print("process>");

        // 4. 数据终端-sink

        // 5. 触发执行-execute
        env.execute("TransformationProcessDemo");
    }

}  