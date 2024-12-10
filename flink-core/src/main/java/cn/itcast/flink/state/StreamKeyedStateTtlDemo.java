package cn.itcast.flink.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义状态State，实现max算子功能，对DataStream数据流中数据，调用keyBy算子后，指定字段获取组内最大值
 * @author xuanyu
 */
public class StreamKeyedStateTtlDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1) ;

        // 2. 数据源-source
        DataStreamSource<Tuple3<String, String, Long>> tupleStream = env.fromElements(
            Tuple3.of("上海", "普陀区", 488L), Tuple3.of("上海", "徐汇区", 212L),
            Tuple3.of("北京", "西城区", 823L), Tuple3.of("北京", "海淀区", 234L),
            Tuple3.of("上海", "杨浦区", 888L), Tuple3.of("上海", "浦东新区", 666L),
            Tuple3.of("北京", "东城区", 323L), Tuple3.of("上海", "黄浦区", 111L)
        );

        // 3. 数据转换-transformation
        // todo： 使用max/maxBy算子对每个组内指定字段获取最大值
        SingleOutputStreamOperator<Tuple3<String, String, Long>> maxStream = tupleStream
            .keyBy(tuple -> tuple.f0)
            .max(2);
        // maxStream.print("max>");

        // todo: 自定义状态，实现max算子功能，获取组内指定字段最大值，先分组，在对组内计算获取最大值，所以说KeyedState键控状态
        SingleOutputStreamOperator<String> stateStream = tupleStream
            .keyBy(tuple -> tuple.f0)
            .map(new RichMapFunction<Tuple3<String, String, Long>, String>() {
                // todo step1. 定义状态，存储每个key对应状态值
                private ValueState<Long> maxState = null ;

                @Override
                public void open(Configuration parameters) throws Exception {
                    // todo step2. 初始化状态，建议在open进行，使用RuntimeContext进行对象实例化
                    // 2-1. 创建状态描述符对象
                    ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("maxState", Long.class);
                    // 2-2. 设置状态ttl生命周期
                    StateTtlConfig stateTtlConfig = StateTtlConfig
                        // 设置状态 时间，表示状态多久没有被更新或使用，将其丢弃（自动自动从内存将其删除）
                        .newBuilder(Time.days(1))
                        // 什么时候更新状态时间戳：key对应状态创建时或更新时
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        // 当key的状态已经为过期，但是还没有从内存中删除，对用户来说可见性：设置不可读
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        // 设置时间语义：当前系统时间，处理数据的时间
                        .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                        .build();
                    stateDescriptor.enableTimeToLive(stateTtlConfig);
                    // 2-3. 实例化状态，传递状态描述符
                    this.maxState = getRuntimeContext().getState(stateDescriptor);
                }

                @Override
                public String map(Tuple3<String, String, Long> value) throws Exception {
                    // 1. 获取组内传递进来数据，指定字段的值
                    Long currentValue = value.f2;
                    // 2. todo step3. 获取key以前的状态值
                    Long historyValue = maxState.value();
                    // 3. 第1次对组内数据计算，key时没有状态，值为null; 如果当前的值 大于 以前的值，更新状态中的值
                    if(null == historyValue || currentValue > historyValue){
                        // todo step4. 更新状态值
                        maxState.update(currentValue);
                    }
                    // 4. 返回计算结果
                    return value.f0 + " -> " + maxState.value();
                }
            });
        stateStream.printToErr("state>");

        // 4. 数据终端-sink

        // 5. 触发执行-execute
        env.execute("StreamKeyedStateDemo");
    }

}  