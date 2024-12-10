package cn.itcast.flink.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 从kafka Topic中消费数据，基于Table API Connector连接器实现；从Kafka Topic队列消费数据，向MySQL表中写入数据
 * @author xuanyu
 */
public class SqlConnectorMysqlSinkDemo {

    public static void main(String[] args) {
        // 1. 构建表的执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inStreamingMode()
            .useBlinkPlanner()
            .build() ;
        TableEnvironment tableEnv = TableEnvironment.create(settings) ;

        // 2. 定义输入表，从Kafka消费数据
        tableEnv.executeSql(
            "CREATE TABLE tbl_log_kafka (\n" +
                "  `user_id` STRING,\n" +
                "  `item_id` INTEGER,\n" +
                "  `behavior` STRING,\n" +
                "  `ts` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'log-topic',\n" +
                "  'properties.bootstrap.servers' = 'node1.itcast.cn:9092,node2.itcast.cn:9092,node3.itcast.cn:9092',\n" +
                "  'properties.group.id' = 'gid-1',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")"
        );

        // 3. 定义输出表，将数据保存到kafka Topic队列中，数据格式为json字符串
        tableEnv.executeSql(
            "CREATE TABLE tbl_log_jdbc_sink (\n" +
                "  `user_id` STRING,\n" +
                "  `item_id` INTEGER,\n" +
                "  `behavior` STRING,\n" +
                "  `ts` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc', \n" +
                "  'url' = 'jdbc:mysql://node1.itcast.cn:3306/db_flink?useSSL=false',\n" +
                "  'table-name' = 'tbl_logs', \n" +
                "  'driver' = 'com.mysql.jdbc.Driver', \n" +
                "  'username' = 'root', \n" +
                "  'password' = '123456', \n" +
                "  'sink.buffer-flush.interval' = '1s', \n" +
                "  'sink.buffer-flush.max-rows' = '1', \n" +
                "  'sink.max-retries' = '5',\n" +
                "  'sink.parallelism' = '4'  \n" +
                ")"
        );

        // 4. 编写SQL，直接查询输入表中数据，插入到输出表中
        tableEnv.executeSql(
            "INSERT INTO tbl_log_jdbc_sink " +
                "SELECT user_id, item_id, behavior, ts FROM tbl_log_kafka"
        );
    }

}
