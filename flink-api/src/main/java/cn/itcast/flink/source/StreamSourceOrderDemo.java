package cn.itcast.flink.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 自定义数据源，实现：每隔1秒随机生成一条订单信息(订单ID、用户ID、订单金额、时间戳)
 *
 * @author xuanyu
 */
public class StreamSourceOrderDemo {

    /**
     * 定义实体类，封装订单数据
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class Order {
        private String id;
        private Integer userId;
        private Double money;
        private Long orderTime;
    }

    /**
     * 自定义数据源，集成抽象类：RichParallelSourceFunction，实现其中方法：run和cancel，实时随机产生交易订单数据
     */
    private static class OrderSource extends RichParallelSourceFunction<Order> {
        // 定义变量，用于标识是否产生数据
        private boolean isRunning = true;

        /**
         * todo 表示产生数据，发送到下游，被进行转换处理
         */
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            // 定义随机数对象
            Random random = new Random();
            while (isRunning) {
                // 创建交易订单对象，表示产生一条数据
                Order order = new Order();
                order.setId(UUID.randomUUID().toString());
                order.setUserId(random.nextInt(10) + 1);
                order.setMoney((double) random.nextInt(100));
                order.setOrderTime(System.currentTimeMillis());
                // 发送订单数据到下游
                ctx.collect(order);

                // 每隔1秒中产生1条数据，让线程休眠1秒钟
                TimeUnit.SECONDS.sleep(1);
            }
        }

        /**
         * todo 当job被取消时，一些操作，比如设置isRunning为false，不会再产生数据
         */
        @Override
        public void cancel() {
            isRunning = false;
        }
    }


    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 2. 数据源-source
        OrderSource orderSource = new OrderSource();
        DataStreamSource<Order> orderDataStream = env.addSource(orderSource);

        // 3. 数据转换-transformation

        // 4. 数据终端-sink
        orderDataStream.printToErr();

        // 5. 触发执行-execute
        env.execute("StreamSourceOrderDemo");
    }

}  