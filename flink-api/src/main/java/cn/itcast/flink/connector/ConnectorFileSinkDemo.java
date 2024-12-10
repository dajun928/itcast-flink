package cn.itcast.flink.connector;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Flink Stream 流计算，将DataStream 保存至文件系统，使用FileSystem Connector
 *
 * @author xuyuan
 */
public class ConnectorFileSinkDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        // TODO: 设置检查点
        env.enableCheckpointing(5000);

        // 2. 数据源-source
        DataStreamSource<String> orderDataStream = env.addSource(new OrderSource());
        //orderDataStream.print();

        // 3. 数据转换-transformation

        // 4. 数据终端-sink
        FileSink<String> fileSink = FileSink
            // 4-1. 设置存储文件格式，Row行存储
            .forRowFormat(
                new Path("datas/order-datas"), new SimpleStringEncoder<String>()
            )
            // 4-2. 设置桶分配政策,默认基于时间的分配器，每小时产生一个桶，格式如下yyyy-MM-dd--HH
            .withBucketAssigner(new DateTimeBucketAssigner<>())
            // 4-3. 设置数据文件滚动策略
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    // 时间间隔
                    .withRolloverInterval(TimeUnit.SECONDS.toMillis(5))
                    // 多久不写入数据时，产生文件
                    .withInactivityInterval(TimeUnit.SECONDS.toMillis(10))
                    // 文件大小
                    .withMaxPartSize(2 * 1024 * 1024)
                    .build()
            )
            // 4-4. 设置文件名称
            .withOutputFileConfig(
                OutputFileConfig.builder()
                    .withPartPrefix("order")
                    .withPartSuffix(".data")
                    .build()
            )
            .build();
        // 4-5. 数据流DataStream添加Sink
        orderDataStream.sinkTo(fileSink);

        // 5. 触发执行
        env.execute("ConnectorFileSinkDemo");
    }

    /**
     * 自定义数据源，实时产生交易订单数据
     */
    private static class OrderSource implements ParallelSourceFunction<String> {
        private boolean isRunning = true;
        private FastDateFormat format = FastDateFormat.getInstance("yyyyMMddHHmmssSSS");

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                // 交易订单
                long timeMillis = System.currentTimeMillis();
                String orderId = format.format(timeMillis) + (10000 + random.nextInt(10000));
                String userId = "u_" + (10000 + random.nextInt(10000));
                double orderMoney = new BigDecimal(random.nextDouble() * 100).setScale(2, RoundingMode.HALF_UP).doubleValue();
                String output = orderId + "," + userId + "," + orderMoney + "," + timeMillis;
                System.out.println(output);
                // 输出
                ctx.collect(output);
                TimeUnit.MILLISECONDS.sleep(100);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}