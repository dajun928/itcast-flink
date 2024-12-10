package cn.itcast.flink.window.session;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Flink Window 窗口计算：基于时间会话窗口SessionWindow，会话超时时间间隔5s
 *      todo 某条数据来了以后，后续超过5s钟没有数据达到，将前面数据当做一个窗口，进行数据计算
 * @author xuanyu
 */
public class TimeSessionWindowDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);

        // 3. 数据转换-transformation
        // 3-1. 过滤和转换数据
        SingleOutputStreamOperator<Integer> mapStream = inputStream
            .filter(line -> line.trim().length() > 0)
            .map(new MapFunction<String, Integer>() {
                @Override
                public Integer map(String value) throws Exception {
                    System.out.println("item: " + value);
                    return Integer.parseInt(value);
                }
            });

        // 3-2. 直接对数据流DataStream设置会话窗口：时间间隔gap=5s
        SingleOutputStreamOperator<String> windowStream = mapStream
            // a. 设置时间会话窗口
            .windowAll(
                ProcessingTimeSessionWindows.withGap(Time.seconds(5))
            )
            // b. 设置窗口函数，计算窗口中数据
            .apply(new AllWindowFunction<Integer, String, TimeWindow>() {
                private FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
                @Override
                public void apply(TimeWindow window, Iterable<Integer> values, Collector<String> out) throws Exception {
                    // 获取窗口信息
                    String windowStart = fastDateFormat.format(window.getStart());
                    String windowEnd = fastDateFormat.format(window.getEnd()) ;

                    // 对窗口中数据进行求和
                    int sum = 0;
                    for (Integer value : values) {
                        sum += value;
                    }

                    // 输出窗口计算结果：求和值
                    String output = "window: [" + windowStart + " ~ " + windowEnd + "], sum = " + sum ;
                    out.collect(output);
                }
            });

        // 4. 数据终端-sink
        windowStream.printToErr();

        // 5. 触发执行-execute
        env.execute("TimeSessionWindowDemo");
    }

}  