package cn.itcast.flink.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 从kafka Topic中消费数据，基于Table API Connector连接器读取加载数据
 * @author xuanyu
 */
public class SqlConnectorKafkaSourceDemo {

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

        // 3. 编写SQL，直接查询表中数据
        tableEnv.executeSql("SELECT * FROM tbl_log_kafka").print();
    }

}
