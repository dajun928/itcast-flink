package cn.itcast.flink.daily;

import lombok.SneakyThrows;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * 案例演示：每日实时大屏统计，模拟电商交易订单数据实时总销售额统计，实时大屏每隔1秒刷新一次
 *      todo: 滚动窗口  + 触发器
 * @author xuanyu
 */
public class StreamDailyOrderReport {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1) ;

        // 2. 数据源-source
        DataStreamSource<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);
/*
数据格式：
    2022-08-04 23:59:01,order_10001,user_1,10.00
    2022-08-04 23:59:04,order_10002,user_2,10.00
    2022-08-04 23:59:58,order_10003,user_4,20.00
    2022-08-05 00:00:01,order_00001,user_2,100.00
    2022-08-05 00:00:12,order_00002,user_4,99.00
 */

        // 3. 数据转换-transformation
        SingleOutputStreamOperator<Tuple2<String, Double>> mapStream = inputStream
            .filter(line -> line.split(",").length == 4)
            // 指定数据中事件时间字段的值，必须为Long类型
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                    .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                        private FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

                        @SneakyThrows
                        @Override
                        public long extractTimestamp(String element, long recordTimestamp) {
                            System.out.println("order -> " + element);
                            String orderTime = element.split(",")[0];
                            Date orderDate = fastDateFormat.parse(orderTime);
                            return orderDate.getTime();
                        }
                    })
            )
            // 提取字段值，封装到二元组
            .map(new MapFunction<String, Tuple2<String, Double>>() {
                @Override
                public Tuple2<String, Double> map(String value) throws Exception {
                    String orderMoney = value.split(",")[3];
                    // 构建二元组返回
                    return Tuple2.of("全国", Double.parseDouble(orderMoney));
                }
            });


        // todo 设置滚动事件时间窗口进行计算
        SingleOutputStreamOperator<String> windowStream = mapStream
            .keyBy(tuple -> tuple.f0)
            .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
            .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
            .apply(new WindowFunction<Tuple2<String, Double>, String, String, TimeWindow>() {
                private FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

                @Override
                public void apply(String key, TimeWindow window,
                                  Iterable<Tuple2<String, Double>> input, Collector<String> out) throws Exception {
                    // 窗口开始时间和结束时间
                    String windowStart = fastDateFormat.format(window.getStart());
                    String windowEnd = fastDateFormat.format(window.getEnd());

                    // 窗口数据计算, todo: 为了简单，直接对double类型数据累加，实际项目必须转换BigDecimal进行求和
                    double total = 0.0;
                    for (Tuple2<String, Double> tuple : input) {
                        total += tuple.f1;
                    }

                    // 输出结果
                    String output = "window[" + windowStart + " ~ " + windowEnd + "], " + key + " = " + total;
                    out.collect(output);
                }
            });

        // 4. 数据终端-sink
        windowStream.printToErr();

        // 5. 触发执行-execute
        env.execute("StreamDailyOrderReport");
    }

}  