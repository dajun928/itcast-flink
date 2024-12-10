package cn.itcast.flink.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * Flink SQL 集成Hive，首先添加依赖，其次创建Catalog，最后读取Hive表中数据
 * @author xuanyu
 */
public class SqlConnectorHiveSourceDemo {

    public static void main(String[] args) {
        // 1. 创建表的执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inBatchMode()
            .useBlinkPlanner()
            .build() ;
        TableEnvironment tableEnv = TableEnvironment.create(settings) ;

        // 2. 创建HiveCatalog对象，传递配置参数
        HiveCatalog hiveCatalog = new HiveCatalog(
            "hiveCatalog",
            "default",
            "flink-sql/src/main/resources/hive-conf",
            "flink-sql/src/main/resources/hadoop-conf",
            "3.1.2"
        );
        // 注册catalog
        tableEnv.registerCatalog("hive_catalog", hiveCatalog);
        // 使用catalog
        tableEnv.useCatalog("hive_catalog");

        // 3. 编写DDL、DML、DQL语句，并执行操作
        tableEnv.executeSql("SHOW DATABASES").print();

        System.out.println("======================================================");

        tableEnv.executeSql("SELECT * FROM db_hive.emp").print();

        System.out.println("======================================================");

        tableEnv.executeSql("SELECT * FROM hive_catalog.db_hive.dept").print();
    }


}
