package cn.itcast.flink.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 从kafka Topic中消费数据，基于Table API Connector连接器实现；从HBase表批量加载数据
 * @author xuanyu
 */
public class SqlConnectorHbaseSourceDemo {

    public static void main(String[] args) {
        // 1. 构建表的执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inBatchMode()
            .useBlinkPlanner()
            .build() ;
        TableEnvironment tableEnv = TableEnvironment.create(settings) ;

        // 3. 定义输入表，关联到HBase表中
        tableEnv.executeSql(
            "CREATE TABLE tbl_log_hbase(\n" +
                "   rowkey STRING,\n" +
                "   info Row<user_id STRING, item_id INTEGER, behavior STRING, ts STRING>,\n" +
                "   PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'hbase-2.2',\n" +
                "  'table-name' = 'htbl_logs',\n" +
                "  'zookeeper.quorum' = 'node1.itcast.cn:2181,node2.itcast.cn:2181,node3.itcast.cn:2181',\n" +
                "  'zookeeper.znode.parent' = '/hbase'\n" +
                ")"
        );

        // 4. 编写SQL，直接查询输入表中数据，插入到输出表中
        tableEnv.executeSql(
            "SELECT rowkey, info.item_id, info.behavior FROM tbl_log_hbase LIMIT 10"
        ).print();
    }

}
