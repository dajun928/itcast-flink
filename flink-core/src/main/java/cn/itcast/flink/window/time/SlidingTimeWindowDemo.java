package cn.itcast.flink.window.time;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 滑动时间窗口案例：每5秒钟统计一次，最近5秒钟内，各个路口通过红绿灯汽车的数量
 * @author xuanyu
 */
public class SlidingTimeWindowDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);
/*
卡口名称,卡口车流量
a,3
a,2
a,7
d,9
b,6
a,5
b,3
e,7
e,4
 */
        // 3. 数据转换-transformation
        // 3-1. 对数据进行转换处理：过滤脏数据，解析数据封装到二元组中
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = inputStream
            .filter(line -> line.trim().split(",").length == 2)
            .map(new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String value) throws Exception {
                    System.out.println("item: " + value);
                    String[] array = value.split(",");
                    String qkName = array[0];
                    Integer qkFlow = Integer.parseInt(array[1]);
                    // 返回二元组
                    return Tuple2.of(qkName, qkFlow);
                }
            });

        // 3-2. 设置窗口和函数，将流式数据划分为窗口计算
        SingleOutputStreamOperator<String> windowStream = mapStream
            // a. 按照卡口名称进行分组
            .keyBy(tuple -> tuple.f0)
            // b. 设置窗口：滑动时间窗口 size = 10s,  slide = 5s
            .window(
                SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))
            )
            // c. 窗口函数，对窗口内数据处理
            .apply(new WindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                private FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
                /**
                 * 窗口数据处理方法
                 * @param key 分组key，此处就是卡口名称
                 * @param window 窗口类型，此处为时间窗口TimeWindow，可以获取窗口开始时间和结束时间
                 * @param input 窗口中数据，封装在迭代器中，遍历数据，进行处理
                 * @param out 窗口计算结果数据收集器，将结果发送到下游
                 */
                @Override
                public void apply(String key, TimeWindow window,
                                  Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
                    // 获取窗口时间信息：开始时间和结束时间
                    String windowStart = fastDateFormat.format(window.getStart());
                    String windowEnd = fastDateFormat.format(window.getEnd());

                    // 对窗口中数据进行计算：统计所有车流量，求和操作
                    /*
                    迭代器input中数据，某个卡口在窗口中数据
                        a,3
                        a,2
                        a,7
                     */
                    int sum = 0 ;
                    for (Tuple2<String, Integer> tuple : input) {
                        sum += tuple.f1 ;
                    }

                    // 输出窗口计算结果
                    String output = "window: [" + windowStart + " ~ " + windowEnd + "], " + key + " = " + sum ;
                    out.collect(output);
                }
            });

        // 4. 数据终端-sink
        windowStream.printToErr();

        // 5. 触发执行-execute
        env.execute("SlidingTimeWindowDemo");
    }

}  