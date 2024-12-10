package cn.itcast.flink.process.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 使用Flink计算引擎实现实时流计算：词频统计WordCount，从TCP Socket消费数据，结果打印控制台
 * @author xuanyu
 */
public class StreamProcessStateDemo {

    public static void main(String[] args) throws Exception {
        //  1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1) ;

        //  2. 数据源-source
        DataStreamSource<String> inputDataStream = env.socketTextStream("node1.itcast.cn", 9999);

        //  3. 数据转换-transformation
        // 3-1. 分割单词
        SingleOutputStreamOperator<String> wordDataStream = inputDataStream.flatMap(
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
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDataStream = wordDataStream.map(
            new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String value) throws Exception {
                    return Tuple2.of(value, 1);
                }
            }
        );

        // 3-3. 按照单词分组，并且组内求和
        //tupleDataStream.keyBy(tuple -> tuple.f0).sum(1);

        /*
                (flink, 1)                      spark ->  (spark, 1), (spark, 1)
        -------------------------> keyBy  -->  flink ->  10 --
                                               hive  ->  (hive, 1)
             todo: 自定义桩体，存储每个单词词频，属于KeyedState键控状态，表示每个Key都有一个状态
         */
        SingleOutputStreamOperator<String> processStream = tupleDataStream
            .keyBy(tuple -> tuple.f0)
            // 调用process转换算子，处理数据
            .process(
                // todo 分组处理函数，表示对分组流KeyedStream中每组中数据进行处理
                new KeyedProcessFunction<String, Tuple2<String, Integer>, String>() {
                    // 定义变量，存储每个Key（单词）状态（词频）
                    private ValueState<Integer> countState = null ;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 实例状态
                        countState = getRuntimeContext().getState(
                            new ValueStateDescriptor<Integer>("countState", Integer.class)
                        );
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value,
                                               Context ctx, Collector<String> out) throws Exception {
                        // value 表示流中每条数据，指的是组内的数据，比如(flink, 1)

                        // a. 获取key以前的词频，从State状态中获取
                        Integer historyValue = countState.value();
                        // b. 获取传递进来的值
                        Integer currentValue = value.f1;

                        // c. 如果key对数据是第1次出现，以前State中值就是null
                        if(null == historyValue){
                            // 直接更新状态
                            countState.update(currentValue);
                        }else {
                            int latestValue = historyValue + currentValue;
                            countState.update(latestValue);
                        }

                        // 计算完成后，进行输出
                        String output = ctx.getCurrentKey() + " -> " + countState.value();
                        out.collect(output);
                    }
                }
            );

        //  4. 数据接收器-sink
        processStream.print();

        //  5. 触发执行-execute
        env.execute("StreamProcessStateDemo");
    }

}
