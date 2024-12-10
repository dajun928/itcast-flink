package cn.itcast.flink.window;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Collection;

/**
 * 使用Flink计算引擎实现实时流计算：词频统计WordCount，从TCP Socket消费数据，结果打印控制台
 * @author xuanyu
 */
public class StreamWindowApiDemo {

    public static void main(String[] args) throws Exception {

        //  1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1) ;

        //  2. 数据源-source
        DataStream<String> inputDataStream = env.socketTextStream("node1.itcast.cn", 9999);

        //  3. 数据转换-transformation
        // 3-1. 分割单词
        DataStream<String> wordDataStream = inputDataStream.flatMap(
            new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String value, Collector<String> out) throws Exception {
                    String[] words = value.split("\\s+");
                    for (String word : words) {
                        out.collect(word);
                    }
                }
            }
        );

        // 3-2. 转换二元组
        DataStream<Tuple2<String, Integer>> tupleDataStream = wordDataStream.map(
            new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String value) throws Exception {
                    return Tuple2.of(value, 1);
                }
            }
        );

        // todo： Window窗口计算 API，第一种【分组流KeyedStream设置窗口】
        tupleDataStream
            // 第1步、设置key，对数据流分组
            .keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
                @Override
                public Object getKey(Tuple2<String, Integer> value) throws Exception {
                    return null;
                }
            })
            // 第2步、设置window窗口
            .window(new WindowAssigner<Tuple2<String, Integer>, Window>() {
                @Override
                public Collection<Window> assignWindows(Tuple2<String, Integer> element, long timestamp, WindowAssignerContext context) {
                    return null;
                }

                @Override
                public Trigger<Tuple2<String, Integer>, Window> getDefaultTrigger(StreamExecutionEnvironment env) {
                    return null;
                }

                @Override
                public TypeSerializer<Window> getWindowSerializer(ExecutionConfig executionConfig) {
                    return null;
                }

                @Override
                public boolean isEventTime() {
                    return false;
                }
            })
            // 第3步、设置窗口函数，对窗口中数据计算
            .apply(new WindowFunction<Tuple2<String, Integer>, Object, Object, Window>() {
                @Override
                public void apply(Object o, Window window, Iterable<Tuple2<String, Integer>> input, Collector<Object> out) throws Exception {

                }
            });


        // todo： Window窗口计算 API，第二种【直接对非分组流DataStream设置窗口】
        tupleDataStream
            // 第1步、设置window窗口
            .windowAll(new WindowAssigner<Tuple2<String, Integer>, Window>() {
                @Override
                public Collection<Window> assignWindows(Tuple2<String, Integer> element, long timestamp, WindowAssignerContext context) {
                    return null;
                }

                @Override
                public Trigger<Tuple2<String, Integer>, Window> getDefaultTrigger(StreamExecutionEnvironment env) {
                    return null;
                }

                @Override
                public TypeSerializer<Window> getWindowSerializer(ExecutionConfig executionConfig) {
                    return null;
                }

                @Override
                public boolean isEventTime() {
                    return false;
                }
            })
            // 第2步、设置窗口函数，对窗口中数据计算
            .apply(new AllWindowFunction<Tuple2<String, Integer>, Object, Window>() {
                @Override
                public void apply(Window window, Iterable<Tuple2<String, Integer>> values, Collector<Object> out) throws Exception {

                }
            });

        //  4. 数据接收器-sink

        //  5. 触发执行-execute
        env.execute("StreamWindowApiDemo");
    }

}
