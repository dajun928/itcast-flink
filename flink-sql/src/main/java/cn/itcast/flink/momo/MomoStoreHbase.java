package cn.itcast.flink.momo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 陌陌社交实时综合案例：实时熊Kafka消费陌陌社交聊天数据，存储到HBase表中，基于Flink SQL实现
 * @author xuanyu
 */
public class MomoStoreHbase {

    public static void main(String[] args) {
        // 1. 表中的执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inStreamingMode()
            .useBlinkPlanner()
            .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings) ;
        // 设置检查点
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("execution.checkpointing.interval", "5000");

        // 2. 创建输入表，从Kafka队列消费数据
        tableEnv.executeSql(
            "CREATE TABLE tbl_momo_msg_kafka (\n" +
                "  `msg_time` STRING,\n" +
                "  `sender_nickyname` STRING,\n" +
                "  `sender_account` STRING,\n" +
                "  `sender_sex` STRING,\n" +
                "  `sender_ip` STRING,\n" +
                "  `sender_os` STRING,\n" +
                "  `sender_phone_type` STRING,\n" +
                "  `sender_network` STRING,\n" +
                "  `sender_gps` STRING,\n" +
                "  `receiver_nickyname` STRING,\n" +
                "  `receiver_ip` STRING,\n" +
                "  `receiver_account` STRING,\n" +
                "  `receiver_os` STRING,\n" +
                "  `receiver_phone_type` STRING,\n" +
                "  `receiver_network` STRING,\n" +
                "  `receiver_gps` STRING,\n" +
                "  `receiver_sex` STRING,\n" +
                "  `msg_type` STRING,\n" +
                "  `distance` STRING,  \n" +
                "  `message` STRING  \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'momo-msg',\n" +
                "  'properties.bootstrap.servers' = 'node1.itcast.cn:9092,node2.itcast.cn:9092,node3.itcast.cn:9092',\n" +
                "  'properties.group.id' = 'momo-gid-1',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'csv',\n" +
                "  'csv.field-delimiter' = '\\001',\n" +
                "  'csv.ignore-parse-errors' = 'true',\n" +
                "  'csv.allow-comments' = 'true'\n" +
                ")"
        );

        // 3. 创建输出表，向Hbase表中写入数据
        tableEnv.executeSql(
            "CREATE TABLE tbl_momo_msg_hbase (\n" +
                " row_key STRING,\n" +
                " info ROW<msg_time STRING, sender_nickyname STRING, sender_account STRING, sender_sex STRING, sender_ip STRING, sender_os STRING, sender_phone_type STRING, sender_network STRING, sender_gps STRING, receiver_nickyname STRING, receiver_ip STRING, receiver_account STRING, receiver_os STRING, receiver_phone_type STRING, receiver_network STRING, receiver_gps STRING, receiver_sex STRING, msg_type STRING, distance STRING, message STRING>,\n" +
                " PRIMARY KEY (row_key) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'htbl_momo_msg_sql',\n" +
                " 'sink.parallelism' = '3',\n" +
                " 'sink.buffer-flush.interval' = '1s',\n" +
                " 'sink.buffer-flush.max-rows' = '1000',\n" +
                " 'sink.buffer-flush.max-size' = '2mb',\n" +
                " 'zookeeper.quorum' = 'node1.itcast.cn:2181,node2.itcast.cn:2181,node3.itcast.cn:2181',\n" +
                " 'zookeeper.znode.parent' = '/hbase'\n" +
                ")"
        );

        // 4. 通过子查询方式，将数据写入到输出表
        tableEnv.executeSql(
            "INSERT INTO tbl_momo_msg_hbase\n" +
                "SELECT\n" +
                "  CONCAT(sender_account, '_', msg_time, '_', receiver_account ) AS row_key,\n" +
                "  ROW(msg_time, sender_nickyname, sender_account, sender_sex, sender_ip, sender_os, sender_phone_type, sender_network, sender_gps, receiver_nickyname, receiver_ip, receiver_account, receiver_os, receiver_phone_type, receiver_network, receiver_gps, receiver_sex, msg_type, distance, message)\n" +
                "FROM tbl_momo_msg_kafka"
        );
    }

}
