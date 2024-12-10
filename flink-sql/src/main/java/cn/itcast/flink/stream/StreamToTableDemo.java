package cn.itcast.flink.stream;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * DataStream数据流与Table表之间相互转换，在实际项目中，可以先基于DataStream记进行数据过滤转换操作，然后转换为Table，最后使用SLQ分析
 * @author xuanyu
 */
public class StreamToTableDemo {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class OrderInfo {
        private String userId;
        private Long ts;
        private Double money;
        private String category;
    }

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1) ;
        // todo a. 创建流式表的执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env ,settings);

        // 2. 数据源-source
        DataStreamSource<String> inputStream = env.readTextFile("datas/order.csv");

        // 3. 数据转换-transformation
        SingleOutputStreamOperator<OrderInfo> orderStream = inputStream.map(new MapFunction<String, OrderInfo>() {
            @Override
            public OrderInfo map(String value) throws Exception {
                // value -> user_001,1621718199,10.1,电脑
                String[] array = value.split(",");
                OrderInfo orderInfo = new OrderInfo();
                orderInfo.setUserId(array[0]);
                orderInfo.setTs(Long.parseLong(array[1]));
                orderInfo.setMoney(Double.parseDouble(array[2]));
                orderInfo.setCategory(array[3]);
                // 返回封装实体类对象
                return orderInfo;
            }
        });
        // todo b. 将DataStream 数据流转换为Table表
        Table orderTable = tableEnv.fromDataStream(orderStream);

        // todo c. 基于SQL查询
        tableEnv.createTemporaryView("tbl_orders", orderTable);
        Table resultTable = tableEnv.sqlQuery(
            "SELECT * FROM tbl_orders"
        );

        // todo: d. 将Table转换为DataStream，Table中额米条数据封装类型Row
        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);

        // 4. 数据终端-sink
        resultStream.printToErr();

        // 5. 触发执行-execute
        env.execute("StreamToTableDemo");
    }

}  