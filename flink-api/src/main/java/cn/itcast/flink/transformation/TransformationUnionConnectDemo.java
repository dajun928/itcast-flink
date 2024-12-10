package cn.itcast.flink.transformation;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * Flink中流计算DataStream转换算子：合并union算子和连接connect算子
 *
 * @author xuanyu
 */
public class TransformationUnionConnectDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<String> dataStream01 = env.fromElements("A", "B", "C", "D");
        DataStreamSource<String> dataStream02 = env.fromElements("aa", "bb", "cc", "dd");
        DataStreamSource<Integer> dataStream03 = env.fromElements(1, 2, 3, 4);

        // 3. 数据转换-transformation
        // todo: 2个流进行union，要求流中数据类型必须相同
        DataStream<String> unionDataStream = dataStream01.union(dataStream02);
        //unionDataStream.printToErr();

        // todo: 2个流进行连接，connect 应用场景 -> 大表与小表维度关联
        ConnectedStreams<String, Integer> connectDataStream = dataStream01.connect(dataStream03);
        // 对连接数据流中数据必须先进行处理，才可以输出，需要调用转换算子，比如map和flatMap算子都可以
        SingleOutputStreamOperator<String> mapDataStream = connectDataStream.map(
            // 函数接口声明：public interface CoMapFunction<IN1, IN2, OUT>，3个泛型，分别表示连接流中2个流数据流数据类型和返回值数据流类型
            new CoMapFunction<String, Integer, String>() {
                // 连接流时，左边数据流（dataStream01）中数据操作方法
                @Override
                public String map1(String value) throws Exception {
                    return "map1: left -> " + value;
                }

                // 连接流式，右边数据流（dataStream03）中数据操作方法
                @Override
                public String map2(Integer value) throws Exception {
                    return "map2: right -> " + value;
                }
            }
        );
        mapDataStream.printToErr();

        // 4. 数据终端-sink

        // 5. 触发执行-execute
        env.execute("TransformationUnionConnectDemo");
    }

}  