package cn.itcast.flink.transformation;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Flink 流计算物理分区算子：对流数据进行分区，决定上游数据如何发送到下游的各个SubTask子任务进行处理
 *
 * @author xuanyu
 */
public class TransformationPartitionDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<Tuple2<Integer, String>> dataStream = env.addSource(
            new RichParallelSourceFunction<Tuple2<Integer, String>>() {
                private boolean isRunning = true;

                @Override
                public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
                    int index = 1;
                    Random random = new Random();
                    String[] chars = new String[]{
                        "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O",
                        "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"
                    };
                    while (isRunning) {
                        Tuple2<Integer, String> tuple = Tuple2.of(index, chars[random.nextInt(chars.length)]);
                        ctx.collect(tuple);

                        TimeUnit.SECONDS.sleep(2);
                        index++;
                    }
                }

                @Override
                public void cancel() {
                    isRunning = false;
                }
            }
        );
        //dataStream.printToErr();

        // 3. 数据转换-transformation
        // todo 1. global 全局，将所有数据发往下游中第1个subTask
        DataStream<Tuple2<Integer, String>> globalDataStream = dataStream.global();
        //globalDataStream.print().setParallelism(3) ;

        // todo 2. broadcast 广播，发送给下游所有SubTask
        DataStream<Tuple2<Integer, String>> broadcastDataStream = dataStream.broadcast();
        //broadcastDataStream.printToErr().setParallelism(3) ;

        // todo 3. forward 向前，上下游算子并行度相同，一对一发送
        //DataStream<Tuple2<Integer, String>> forwardDataStream = dataStream.setParallelism(3).forward();
        //forwardDataStream.printToErr().setParallelism(3) ;

        // todo 4. shuffle 随机，发送下游时，随机选择一个subTask
        //DataStream<Tuple2<Integer, String>> shuffleDataStream = dataStream.shuffle();
        //shuffleDataStream.printToErr().setParallelism(3) ;

        // todo 5. rebalance 均衡，采用轮询机制发送到下游各个subTask任务
        DataStream<Tuple2<Integer, String>> rebalanceDataStream = dataStream.rebalance();
        //rebalanceDataStream.printToErr().setParallelism(3) ;

        // todo 6. rescale 局部均衡，本地轮询机制
        //DataStream<Tuple2<Integer, String>> rescaleDataStream = dataStream.setParallelism(4).rescale();
        //rebalanceDataStream.printToErr().setParallelism(2);

        // todo 7. partitionCustom 自定义分区规则
        DataStream<Tuple2<Integer, String>> customDataStream = dataStream.partitionCustom(
            // 2. 依据指定key，决定如何对数据进行物理分区，怎么发送上游数据到下游的subTask
            new Partitioner<Integer>() {
                @Override
                public int partition(Integer key, int numPartitions) {
                    // 如果是奇数：1  -> 表示数据发送到下游的第2个subTask，  如果是偶数：0  -> 表示发送到下游的第1个SubTask
                    return key % 2;
                }
            },
            // 1. 从数据流中每条数据获取如何分发数据的key
            new KeySelector<Tuple2<Integer, String>, Integer>() {
                @Override
                public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                    return value.f0;
                }
            }
        );
        customDataStream.printToErr().setParallelism(2);

        // 4. 数据终端-sink

        // 5. 触发执行-execute
        env.execute("TransformationPartitionDemo");
    }

}  