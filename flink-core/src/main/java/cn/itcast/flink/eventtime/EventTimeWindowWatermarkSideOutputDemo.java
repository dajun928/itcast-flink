package cn.itcast.flink.eventtime;

import lombok.SneakyThrows;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Date;

/**
 * Flink Window窗口计算：基于事件时间窗口EventTimeWindow，此处滚动时间窗口
 *      todo: 要求数据中必须自带数据产生的时间字段，也就是数据事件时间，将流式数据划分为窗口时，依据事件时间划分
 * @author xuanyu
 */
public class EventTimeWindowWatermarkSideOutputDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1) ;

        // 2. 数据源-source
        DataStreamSource<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);
/*
2022-04-01 09:00:01,a,1
2022-04-01 09:00:02,a,1
2022-04-01 09:00:05,a,1

2022-04-01 09:00:10,a,1

2022-04-01 09:00:11,a,1
2022-04-01 09:00:14,b,1
2022-04-01 09:00:15,b,1
 */

        // 3. 数据转换-transformation
        // 3-1. 过滤脏数据，并且指定数据中事件时间字段的值（必须转换为Long类型）
        SingleOutputStreamOperator<String> timeStream = inputStream
            .filter(line -> line.trim().split(",").length == 3)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    // todo 考虑数据乱序问题，设置允许最大乱序时间，也就是等待时间：2s (为了测试设置这么大）
                    .<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    // todo step1. 提取数据中事件时间字段的值，转换为Long类型
                    .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                        private FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss") ;
                        @SneakyThrows
                        @Override
                        public long extractTimestamp(String element, long recordTimestamp) {
                            // element -> 表示每条数据：2022-04-01 09:00:11,a,1
                            System.out.println("element: " + element);

                            // 分割字符串
                            String[] array = element.split(",");
                            // 获取数据产生的时间，事件时间
                            String eventTime = array[0];
                            // 转换Date日期类型
                            Date eventDate = fastDateFormat.parse(eventTime);
                            // 获取日期时间戳并返回
                            return eventDate.getTime();
                        }
                    })
            );

        // 3-2. 对数据进行解析封装操作，获取卡口名称和卡口流量，放入二元组中
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = timeStream.map(
            new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String value) throws Exception {
                    // value 每条数据 -> 2022-04-01 09:00:01,a,1
                    String[] array = value.split(",");
                    String qkName = array[1] ;
                    Integer qkFlow = Integer.parseInt(array[2]) ;
                    // 创建二元组对象，返回
                    return Tuple2.of(qkName, qkFlow);
                }
            }
        );

        // todo：定义乱序迟到数据输出标签
        OutputTag<Tuple2<String, Integer>> lateOutputTag = new OutputTag<Tuple2<String, Integer>>("late-data"){} ;


        // 3-3. 设置滚动事件时间窗口，进行窗口数据计算
        SingleOutputStreamOperator<String> windowStream = mapStream
            // a. 按照卡口名称分组，使用keyBy算子
            .keyBy(tuple -> tuple.f0)
            // b. todo step2. 指定事件时间窗口 size = slide = 5s
            .window(
                TumblingEventTimeWindows.of(Time.seconds(5))
            )
            // todo 设置乱序迟到数据，侧边流输出
            .sideOutputLateData(lateOutputTag)
            // c. 窗口函数，对窗口中数据计算
            .apply(new WindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                private FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

                @Override
                public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
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
                    int sum = 0;
                    for (Tuple2<String, Integer> tuple : input) {
                        sum += tuple.f1;
                    }

                    // 输出窗口计算结果
                    String output = "window: [" + windowStart + " ~ " + windowEnd + "], " + key + " = " + sum;
                    out.collect(output);
                }
            });

        // 4. 数据终端-sink
        windowStream.printToErr();

        // todo 依据侧边流输出标签，获取侧边流，将数据打印控制台
        DataStream<Tuple2<String, Integer>> lateStream = windowStream.getSideOutput(lateOutputTag);
        lateStream.print("late>");

        // 5. 触发执行-execute
        env.execute("EventTimeWindowDemo");
    }

}  