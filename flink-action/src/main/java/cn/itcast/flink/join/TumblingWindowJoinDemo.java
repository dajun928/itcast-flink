package cn.itcast.flink.join;

import com.sun.org.apache.xpath.internal.operations.Or;
import lombok.SneakyThrows;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Date;

/**
 * Flink 双流JOIN，基于window窗口实现join，案例演示【滚动事件时间窗口JOIN】
 *      todo: orderStream -> 订单数据流， detailStream -> 订单详情数据流
 * @author xuanyu
 */
public class TumblingWindowJoinDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1) ;

        // 2. 数据源-source
        // 2-1. order订单数据流 -> 9999
        DataStreamSource<String> rawOrderStream = env.socketTextStream("node1.itcast.cn", 9999);

        // 2-2. detail 订单详情数据流 -> 8888
        DataStreamSource<String> rawDetailStream = env.socketTextStream("node1.itcast.cn", 8888);
/*
2022-04-05 06:00:00,order_101,user_1,shanghai-haizhou,60.00
-----------------------------------------------------
2022-04-05 06:00:01,order_101,detail_1,tomato,4,17.50
2022-04-05 06:00:01,order_101,detail_2,potato,2,12.50
2022-04-05 06:00:01,order_101,detail_3,egg,20,30.00


2022-04-05 06:00:07,order_102,user_2,shanghai-changda,100.00
-----------------------------------------------------
2022-04-05 06:00:07,order_102,detail_1,milk,1,64.80
2022-04-05 06:00:08,order_102,detail_3,pig,1,35.20


2022-04-05 06:00:12,order_103,user_3,shanghai-changtai,45.00
-----------------------------------------------------
2022-04-05 06:00:12,order_103,detail_1,milk,1,45.00
 */
        // 3. 数据转换-transformation
        /*
         window join 窗口关联, 按照事件时间EventTime划分窗口，并且滚动窗口
         3-1. 对【订单数据流】中订单数据处理
            过滤、解析封装实体类对象，设置数据中事件时间字段（不考虑乱序数据）
         3-2. 对【订单详情数据流】中订单详情数据处理
            过滤、解析封装实体类对象，设置数据中事件时间字段（不考虑乱序数据）
         3-3. 读2个流进行窗口join，基于事件时间的滚动窗口，定义JoinFunction函数
         */
        // 3-1. 对【订单数据流】中订单数据处理
        SingleOutputStreamOperator<MainOrder> orderStream = rawOrderStream
            .filter(line -> line.trim().split(",").length == 5)
            // 设置每条数据的事件时间字段值，不考虑乱序延迟迟到数据的处理，直接丢弃
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
            // 解析封装到实体类对象
            .map(new MapFunction<String, MainOrder>() {
                @Override
                public MainOrder map(String value) throws Exception {
                    // 订单数据：2022-04-05 06:00:12,order_103,user_3,shanghai-changtai,45.00
                    String[] array = value.split(",");
                    MainOrder mainOrder = new MainOrder() ;
                    mainOrder.setOrderTime(array[0]);
                    mainOrder.setOrderId(array[1]);
                    mainOrder.setUserId(array[2]);
                    mainOrder.setAddress(array[3]);
                    mainOrder.setOrderMoney(Double.parseDouble(array[4]));
                    // 返回实体类对象
                    return mainOrder;
                }
            });

        // 3-2. 对【订单详情数据流】中订单详情数据处理
        SingleOutputStreamOperator<DetailOrder> detailStream = rawDetailStream
            .filter(line -> line.trim().split(",").length == 6)
            // 设置每条数据的事件时间字段值，不考虑乱序延迟迟到数据的处理，直接丢弃
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                    .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                        private FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

                        @SneakyThrows
                        @Override
                        public long extractTimestamp(String element, long recordTimestamp) {
                            System.out.println("detail -> " + element);
                            String orderTime = element.split(",")[0];
                            Date orderDate = fastDateFormat.parse(orderTime);
                            return orderDate.getTime();
                        }
                    })
            )
            // 解析封装到实体类对象
            .map(new MapFunction<String, DetailOrder>() {
                @Override
                public DetailOrder map(String value) throws Exception {
                    // 2022-04-05 06:00:12,order_103,detail_1,milk,1,45.00
                    String[] array = value.split(",");
                    DetailOrder detailOrder = new DetailOrder();
                    detailOrder.setDetailTime(array[0]);
                    detailOrder.setOrderId(array[1]);
                    detailOrder.setDetailId(array[2]);
                    detailOrder.setGoodsName(array[3]);
                    detailOrder.setGoodsNumber(Integer.parseInt(array[4]));
                    detailOrder.setDetailMoney(Double.parseDouble(array[5]));
                    // 返回封装实体类对象
                    return detailOrder;
                }
            });

        // 3-3. 读2个流进行窗口join，基于事件时间的滚动窗口，定义JoinFunction函数
        DataStream<DwdOrder> joinStream = orderStream
            // 第1步、join关联数据流
            .join(detailStream)
            // 第2步、指定流中关联key
            // .where(order -> order.getOrderId()).equalTo(detail -> detail.getOrderId())
            .where(MainOrder::getOrderId).equalTo(DetailOrder::getOrderId)
            // 第3步、设置窗口window，size=5s
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            // 第4步、窗口数据join操作，指定JoinFunction
            .apply(new JoinFunction<MainOrder, DetailOrder, DwdOrder>() {
                @Override
                public DwdOrder join(MainOrder mainOrder, DetailOrder detailOrder) throws Exception {
                    /*
                    2022-04-05 06:00:00,order_101,user_1,shanghai-haizhou,60.00
                    -----------------------------------------------------
                    2022-04-05 06:00:01,order_101,detail_1,tomato,4,17.50
                     */
                    DwdOrder dwdOrder = new DwdOrder();
                    dwdOrder.setOrderId(mainOrder.getOrderId());

                    // 订单数据字段设置
                    dwdOrder.setOrderTime(mainOrder.getOrderTime());
                    dwdOrder.setUserId(mainOrder.getUserId());
                    dwdOrder.setAddress(mainOrder.getAddress());
                    dwdOrder.setOrderMoney(mainOrder.getOrderMoney());

                    // 订单详情数据字段设置
                    dwdOrder.setDetailId(detailOrder.getDetailId());
                    dwdOrder.setDetailOrderTime(detailOrder.getDetailTime());
                    dwdOrder.setDetailMoney(detailOrder.getDetailMoney());
                    dwdOrder.setGoodsName(detailOrder.getGoodsName());
                    dwdOrder.setGoodsNumber(detailOrder.getGoodsNumber());

                    // 返回关联数据
                    return dwdOrder;
                }
            });

        // 4. 数据终端-sink
        joinStream.printToErr();

        // 5. 触发执行-execute
        env.execute("TumblingWindowJoinDemo");
    }

}  