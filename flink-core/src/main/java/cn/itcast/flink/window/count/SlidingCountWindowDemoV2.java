package cn.itcast.flink.window.count;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * Flink Window窗口计算：滑动计数窗口案例演示【每4条数据对最近3条数据进行统计计算】
 * @author xuanyu
 */
public class SlidingCountWindowDemoV2 {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);
/*
1
1
1
2
3
4
5
6
4
3
 */
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
        
        // 3-2. 直接对DataStream数据流设置窗口，进行窗口数据计算处理
        SingleOutputStreamOperator<String> windowStream = mapStream
            // a. 直接设置窗口：滑动计数窗口
            .countWindowAll(3, 4)
            // b. 设置窗口函数，计算窗口中数据
            .apply(new AllWindowFunction<Integer, String, GlobalWindow>() {
                @Override
                public void apply(GlobalWindow window, Iterable<Integer> values, Collector<String> out) throws Exception {
                    // 遍历窗口中数据，进行累加求和
                    int sum = 0 ;
                    for (Integer value : values) {
                        sum += value;
                    }

                    // 输出窗口中计算结果
                    out.collect("sum = " + sum);
                }
            });

        // 4. 数据终端-sink
        windowStream.printToErr();

        // 5. 触发执行-execute
        env.execute("TumblingCountWindowDemo");
    }

}  