package cn.itcast.flink.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 从kafka Topic中消费数据，基于Table API Connector连接器实现；从Kafka Topic队列消费数据，向HBase表中写入数据
 *      todo: 基于流式方式，编写Flink SQl程序，相当于DataStream程序
 * @author xuanyu
 */
public class SqlConnectorHbaseSinkDemo {

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
            "CREATE TABLE tbl_log_hbase_sink (\n" +
                "   rowkey STRING,\n" +
                "   info Row<user_id STRING, item_id INTEGER, behavior STRING, ts STRING>,\n" +
                "   PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'hbase-2.2',\n" +
                "  'table-name' = 'htbl_logs',\n" +
                "  'zookeeper.quorum' = 'node1.itcast.cn:2181,node2.itcast.cn:2181,node3.itcast.cn:2181',\n" +
                "  'zookeeper.znode.parent' = '/hbase',\n" +
                "  'sink.buffer-flush.max-size' = '1mb',\n" +
                "  'sink.buffer-flush.max-rows' = '1',\n" +
                "  'sink.buffer-flush.interval' = '1s',\n" +
                "  'sink.parallelism' = '3'\n" +
                ")"
        );

        // 4. 编写SQL，直接查询输入表中数据，插入到输出表中
        tableEnv.executeSql(
            "INSERT INTO tbl_log_hbase_sink " +
                "SELECT CONCAT(user_id, '#', ts) AS rowkey , Row(user_id, item_id, behavior, ts) FROM tbl_log_kafka"
        );
    }

}
