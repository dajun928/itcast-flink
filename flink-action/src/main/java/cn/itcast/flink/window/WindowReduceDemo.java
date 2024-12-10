package cn.itcast.flink.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 滚动时间窗口案例演示：实时交通卡口流量统计，每隔5秒统计最近5秒钟各个卡口流量
 * @author xuyuan
 */
public class WindowReduceDemo {

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
        SingleOutputStreamOperator<Tuple2<String, Integer>> windowStream = mapStream
            // a. 设置分组key，按照卡口分组
            .keyBy(tuple -> tuple.f0)
            // b. 设置窗口，并且为滚动窗口：size=slide
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            // c. 窗口计算，窗口函数
            .reduce(
                new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tmp,
                                                          Tuple2<String, Integer> item) throws Exception {
                        /*
                            todo: 调用reduce算子时，要求聚合返回值数据类型，与数据类型相同

                            tmp:
                                对窗口中数据聚合时，存储聚合中间结果变量，类型与窗口中数据类型一致
                                todo: 如果数据为窗口中第1条数据，直接赋值给tmp，不会调用reduce方法增量聚合
                                    (a, 10)

                            item:
                                窗口中每条数据, todo: 从窗口中第2条数据开始赋值
                                    (a, 20)
                         */
                        // step1. 获取以前聚合值
                        Integer historyValue = tmp.f1;
                        // step2. 获取当前数据中值
                        Integer currentValue = item.f1;
                        // step3. 累加当前数据与以前数据值
                        int latestValue = historyValue + currentValue;

                        System.out.println(
                            "以前聚合结果: tmp = " + tmp + ", 当前数据: item = " + item + ", 聚合后值: latest = " + latestValue
                        );
                        // 返回窗口计算结果
                        return Tuple2.of(tmp.f0, latestValue);
                    }
                }
            );

        // 4. 数据终端-sink
        windowStream.printToErr();

        // 5. 触发执行-execute
        env.execute("WindowApplyDemo");
    }

}  