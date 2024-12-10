package cn.itcast.flink.order;

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
 * 案例演示：每次统计最近10秒各个用户订单销售额，最大允许乱序时间：2秒，最大允许延迟时间：3秒，迟到很久数据侧边输出
 * @author xuanyu
 */
public class StreamOrderWindowReport {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);
/*
订单ID, 用户ID, 订单金额, 订单时间

o_101,u_121,11.50,2022-04-05 10:00:02
o_102,u_121,59.50,2022-04-05 10:00:04
o_103,u_121,4.00,2022-04-05 10:00:07
o_104,u_121,22.25,2022-04-05 10:00:10

o_105,u_121,37.35,2022-04-05 10:00:09

o_106,u_121,33.40,2022-04-05 10:00:11
o_107,u_121,4.00,2022-04-05 10:00:12

o_108,u_121,29.10,2022-04-05 10:00:08

o_109,u_121,25.20,2022-04-05 10:00:15
o_110,u_121,58.80,2022-04-05 10:00:06

o_111,u_121,80.90,2022-04-05 10:00:20
o_112,u_121,46.10,2022-04-05 10:00:22
 */

        // 3. 数据转换-transformation
        /*
            3-1. 过滤、解析、封装数据到实体类对象
            3-2. 指定事件时间字段的值，类型为long
            3-3. 对数据流进行分组，设置窗口进行计算
         */
        // 3-1. 过滤、解析、封装数据到实体类对象
        SingleOutputStreamOperator<OrderEvent> orderStream = inputStream
            .filter(line -> line.trim().split(",").length == 4)
            .map(new MapFunction<String, OrderEvent>() {
                @Override
                public OrderEvent map(String value) throws Exception {
                    System.out.println("order -> " + value);
                    // 字符串分割
                    String[] array = value.split(",");
                    // 创建封装实体类对象
                    OrderEvent orderEvent = new OrderEvent();
                    orderEvent.setOrderId(array[0]);
                    orderEvent.setUserId(array[1]);
                    orderEvent.setOrderMoney(Double.parseDouble(array[2]));
                    orderEvent.setOrderTime(array[3]);
                    // 返回结果数据
                    return orderEvent;
                }
            });

        //  3-2. 指定事件时间字段的值，类型为long
        SingleOutputStreamOperator<OrderEvent> timeStream = orderStream.assignTimestampsAndWatermarks(
            WatermarkStrategy
                // todo: 考虑乱序时间处理，设置最大允许乱序时间 = 2s
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    private FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
                    @SneakyThrows
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        // element -> o_112,u_121,46.10,2022-04-05 10:00:22
                        String orderTime = element.getOrderTime();
                        // 转换字符串为日期Date类型
                        Date orderDate = fastDateFormat.parse(orderTime);
                        // 返回时间戳
                        return orderDate.getTime();
                    }
                })
        );

        // 迟到数据侧边输出标签
        OutputTag<Tuple2<String, Double>> lateOutput = new OutputTag<Tuple2<String, Double>>("late-data"){} ;
        // 3-3. 对数据流进行分组，设置窗口进行计算
        /*
                每次统计最近10秒各个用户订单销售额
                            |
               滚动窗口：每10秒统计最近10秒的数据，分组流窗口计算：各个用户
                            |
               窗口中数据计算结果形式：
                    【字符串String】 ，可以，但是太low

                    【最好，将结果数据封装到实体类对象，包含哪些字段？】
                        window_start, window_end, user_id, total_money
         */
        SingleOutputStreamOperator<OrderReport> reportStream = timeStream
            // 提取计算使用字段，封装到二元组中
            .map(new MapFunction<OrderEvent, Tuple2<String, Double>>() {
                @Override
                public Tuple2<String, Double> map(OrderEvent value) throws Exception {
                    return Tuple2.of(value.getUserId(), value.getOrderMoney());
                }
            })
            // a. 按照用户分组
            .keyBy(tuple -> tuple.f0)
            // b. 窗口计算：滚动事件时间窗口
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            // todo: 设置最大允许延迟数据时间=3s，当窗口触发计算后，保存窗口数据时间，在此时间范围内，窗口延迟数据到达，依然触发窗口计算
            .allowedLateness(Time.seconds(3))
            // todo: 迟到数据，进行侧边流输出
            .sideOutputLateData(lateOutput)
            // c. 窗口函数，对窗口中数据计算
            .apply(new WindowFunction<Tuple2<String, Double>, OrderReport, String, TimeWindow>() {
                private FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
                @Override
                public void apply(String key, TimeWindow window,
                                  Iterable<Tuple2<String, Double>> input, Collector<OrderReport> out) throws Exception {
                    // step1. 获取窗口开始时间和结束时间
                    String windowStart = fastDateFormat.format(window.getStart());
                    String windowEnd = fastDateFormat.format(window.getEnd());
                    // step2. 对窗口中数据计算
                    double sum = 0.0 ;
                    for (Tuple2<String, Double> tuple2 : input) {
                        sum += tuple2.f1 ;
                    }
                    // step3. 创建窗口计算结果实例对象
                    OrderReport orderReport = new OrderReport();
                    orderReport.setUserId(key);
                    orderReport.setWindowStart(windowStart);
                    orderReport.setWindowEnd(windowEnd);
                    orderReport.setTotalMoney(sum);
                    // step4. 输出窗口计算结果
                    out.collect(orderReport);
                }
            });

        // 4. 数据终端-sink
        reportStream.printToErr();

        // todo： 依据标签获取侧边流数据中迟到数据
        DataStream<Tuple2<String, Double>> lateDataStream = reportStream.getSideOutput(lateOutput);
        lateDataStream.print("late>");

        // 5. 触发执行-execute
        env.execute("StreamOrderWindowReport");
    }

}  