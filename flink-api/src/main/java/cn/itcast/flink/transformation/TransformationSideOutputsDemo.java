package cn.itcast.flink.transformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Flink 流计算中转换算子：使用侧边输出进行分割主流中数据，到侧边输出流（分割子流）
 *
 * @author xuanyu
 */
public class TransformationSideOutputsDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<Long> inputDataStream = env.fromSequence(1, 10);

        // 3. 数据转换-transformation
        /*
            对数据流进行划分，将奇数数据放到流中，将偶数数据放到流中，原来数据流中数据继续处理，比如平方输出
                使用sideOutput侧边流方式实现
         */
        // step1. 定义侧边输出流标签
        OutputTag<Long> oddTag = new OutputTag<Long>("side-odd") {
        };
        OutputTag<Long> evenTag = new OutputTag<Long>("side-even") {
        };

        // step2. 调用process算子，对流中数据处理和打标签
        SingleOutputStreamOperator<String> mainStream = inputDataStream.process(
            new ProcessFunction<Long, String>() {
                // 处理流中每条数据，属于底层数据处理算子
                @Override
                public void processElement(Long value, Context ctx, Collector<String> out) throws Exception {
                    // a. 对流中每条数据进行处理 -> 每条数据进行平方
                    double powValue = Math.pow(value, 2);
                    out.collect("main: " + powValue);

                    // b. 判断流中数据是奇数还是偶数，打上对应标签
                    if (value % 2 == 0) {
                        ctx.output(evenTag, value);
                    } else {
                        ctx.output(oddTag, value);
                    }
                }
            }
        );

        // 4. 数据终端-sink
        mainStream.printToErr();

        // step3. 获取侧边输出流，依据标签获取数据
        DataStream<Long> oddDataStream = mainStream.getSideOutput(oddTag);
        oddDataStream.print("odd>");

        DataStream<Long> evenDataStream = mainStream.getSideOutput(evenTag);
        evenDataStream.print("even>");

        // 5. 触发执行-execute
        env.execute("TransformationSideOutputsDemo");
    }

}  