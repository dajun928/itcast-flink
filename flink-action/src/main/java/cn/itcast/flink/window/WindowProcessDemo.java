package cn.itcast.flink.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 滚动时间窗口案例演示：实时交通卡口流量统计，每隔5秒统计最近5秒钟各个卡口流量
 * @author xuyuan
 */
public class WindowProcessDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);

/*
数据：
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
        // 3-1. 对数据进行转换处理: 过滤脏数据，解析封装到二元组中
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = inputStream
            .filter(line -> line.trim().split(",").length == 2)
            .map(new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String line) throws Exception {
                    System.out.println("每条卡口流量数据: " + line);
                    String[] array = line.trim().split(",");
                    Integer qkFlow = Integer.parseInt(array[1]);
                    return Tuple2.of(array[0], qkFlow);
                }
            });

        // todo: 3-2. 窗口计算，每隔5秒计算最近5秒各个卡口流量
        SingleOutputStreamOperator<String> windowStream = mapStream
            // a. 设置分组key，按照卡口分组
            .keyBy(tuple -> tuple.f0)
            // b. 设置窗口，并且为滚动窗口：size=slide
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            // c. 窗口计算，窗口函数
            .process(
                new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                        // 从上下文对象中，获取窗口
                        TimeWindow window = context.window();
                        String winStart = this.format.format(window.getStart());
                        String winEnd = this.format.format(window.getEnd());

                        // 对窗口中数据进行统计：求和
                        int sum = 0;
                        for (Tuple2<String, Integer> tuple : elements) {
                            sum += tuple.f1;
                        }

                        // 输出结果数据
                        String output = "window: [" + winStart + " ~ " + winEnd + "], " + key + " = " + sum;
                        out.collect(output);
                    }
                }
            );

        // 4. 数据终端-sink
       windowStream.printToErr();

        // 5. 触发执行-execute
        env.execute("WindowApplyDemo");
    }

}  