package cn.itcast.flink.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Flink 流式计算程序检查点Checkpoint配置
 * @author xuanyu
 */
public class StreamCheckpointSettingDemo {

    /**
     * 自定义数据源，每隔1秒产生1条数据
     */
    private static class DataSource extends RichParallelSourceFunction<String> {
        private boolean isRunning = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                // 发送数据
                ctx.collect("spark flink flink");

                // 每隔1秒发送1条数据
                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        Configuration configuration = new Configuration();
        configuration.setString("rest.port", "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        // todo: 启动Checkpoint检查点和设置属性值
        setEnvCheckpoint(env) ;

        // 2. 数据源-source
        DataStreamSource<String> dataStream = env.addSource(new DataSource());

        // 3. 数据转换-transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> outputStream = dataStream
            .flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String value, Collector<String> out) throws Exception {
                    String[] words = value.split("\\s+");
                    for (String word : words) {
                        out.collect(word);
                    }
                }
            })
            .map(new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String value) throws Exception {
                    return Tuple2.of(value, 1);
                }
            })
            .keyBy(tuple -> tuple.f0).sum(1);

        // 4. 数据终端-sink
        outputStream.printToErr();

        // 5. 触发执行-execute
        env.execute("StreamCheckpointSettingDemo");
    }


    /**
     * 对Flink 流式Job启动Checkpoint检查和属性相关属性值
     */
    private static void setEnvCheckpoint(StreamExecutionEnvironment env) {
        // 1. 启动Checkpoint检查点机制，进行容灾恢复
        env.enableCheckpointing(1000) ;

        // 2. 设置StateBackend和CheckpointStorage
        env.setStateBackend(new HashMapStateBackend()) ;
        env.getCheckpointConfig().setCheckpointStorage(
            new FileSystemCheckpointStorage("file:///D:/BigDataBxg04/ckpts")
        );

        // 3. 设置Checkpoint检查点属性值
        // 相邻2个Checkpoint之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 最多运行Job进行Checkpoint检查点失败次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        // 同一时刻，一个Job最多执行1个Checkpoint
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 当Job作业运行时被取消，保存State的Checkpoint是否删除还是保留，默认时删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        // Checkpoint超时时间，如果超过，即此次Checkpoint检查点失败
        env.getCheckpointConfig().setCheckpointTimeout(5 * 60 * 1000);
        // Checkpoint时语义性
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    }

}  