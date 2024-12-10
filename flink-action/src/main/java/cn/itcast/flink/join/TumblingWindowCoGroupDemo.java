package cn.itcast.flink.join;

import lombok.SneakyThrows;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * Flink 双流JOIN，基于window窗口实现cogroup，案例演示【滚动事件时间窗口cogroup联合分组】
 *      todo: orderStream -> 订单数据流， detailStream -> 订单详情数据流
 * @author xuanyu
 */
public class TumblingWindowCoGroupDemo {

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


2022-04-05 06:00:17,order_104,user_4,shanghai-heima,0.00
-----------------------------------------------------


2022-04-05 06:00:22,order_105,user_5,shanghai-xiaweiyi,60.00
-----------------------------------------------------
2022-04-05 06:00:23,order_105,detail_1,milk,1,60.00
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
            .coGroup(detailStream)
            // 第2步、指定流中关联key
            .where(MainOrder::getOrderId).equalTo(DetailOrder::getOrderId)
            // 第3步、设置窗口window，size=5s
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            // 第4步、窗口数据join操作
            .apply(new CoGroupFunction<MainOrder, DetailOrder, DwdOrder>() {
                /**
                 * 对2个流窗口中匹配key的数据进行join操作，可以实现inner join和oute join（left、right和full）
                 * @param first 表示左边流中key相同的数据，此处指定的是订单表数据
                 * @param second 表示右边流中key相同的数据，此处指定的是订单详情表数据
                 */
                @Override
                public void coGroup(Iterable<MainOrder> first,
                                    Iterable<DetailOrder> second,
                                    Collector<DwdOrder> out) throws Exception {
                    //  todo： 以左表为准，遍历数据
                    for (MainOrder mainOrder : first) {
                        DwdOrder dwdOrder = new DwdOrder();
                        dwdOrder.setOrderId(mainOrder.getOrderId());
                        dwdOrder.setOrderTime(mainOrder.getOrderTime());
                        dwdOrder.setUserId(mainOrder.getUserId());
                        dwdOrder.setAddress(mainOrder.getAddress());
                        dwdOrder.setOrderMoney(mainOrder.getOrderMoney());

                        // 定义变量，表示是否与右表关联
                        boolean isJoin = false;

                        // todo: 直接遍历右表数据，当且仅当右表中有数据时，才会执行遍历
                        for (DetailOrder detailOrder : second) {
                            isJoin = true;
                            // 如果有数据，就相当于关联，设置属性值
                            dwdOrder.setDetailId(detailOrder.getDetailId());
                            dwdOrder.setDetailOrderTime(detailOrder.getDetailTime());
                            dwdOrder.setDetailMoney(detailOrder.getDetailMoney());
                            dwdOrder.setGoodsName(detailOrder.getGoodsName());
                            dwdOrder.setGoodsNumber(detailOrder.getGoodsNumber());

                            // 输出关联数据
                            out.collect(dwdOrder);
                        }

                        // 如果右表没有阈值key相关的数据，说明没有关联成功，单独输出左右表数据，dodo：类似左外连接
                        if(!isJoin){
                            out.collect(dwdOrder);
                        }
                    }
                }
            });

        // 4. 数据终端-sink
        joinStream.printToErr();

        // 5. 触发执行-execute
        env.execute("TumblingWindowJoinDemo");
    }

}  