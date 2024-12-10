package cn.itcast.flink.window;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import static org.apache.flink.table.api.Expressions.*;

/**
 * 分组窗口GroupWindow案例：以滚动事件时间窗口为例，从Socket实时消费卡口流量数据，进行卡口流量实时统计，将结果打印控制台
 * @author xuanyu
 */
public class GroupWindowTableApiDemo {

    public static void main(String[] args) {
        // 1. 表的执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inStreamingMode()
            .useBlinkPlanner()
            .build() ;
        TableEnvironment tableEnv = TableEnvironment.create(settings) ;

        // 2. 数据源，创建输入表
        tableEnv.executeSql(
            "CREATE TABLE tbl_road_records (\n" +
                "  record_time TIMESTAMP(3),\n" +
                "  road_id STRING,\n" +
                "  record_count INT,\n" +
                "  WATERMARK FOR record_time AS record_time - INTERVAL '0' SECOND\n" +
                ") WITH (\n" +
                "  'connector' = 'socket',\n" +
                "  'hostname' = 'node1.itcast.cn',\n" +
                "  'port' = '9999', \n" +
                "  'format' = 'csv',\n" +
                "  'csv.ignore-parse-errors' = 'true'\n" +
                ")"
        );

        // 3. 编写Table API分析数据：基于事件时间的滚动窗口计算，size = 5s
        Table resultTable = tableEnv
            // a. 指定表的名称
            .from("tbl_road_records")
            // b. 设置滚动窗口
            .window(
                Tumble
                    .over(lit(5).seconds())
                    .on($("record_time"))
                    .as("win")
            )
            // c. 分组，先窗口，再业务字段
            .groupBy($("win"), $("road_id"))
            // d. 选择字段和使用聚合函数聚合数据
            .select(
                $("win").start().as("win_start"),
                $("win").end().as("win_end"),
                $("road_id"),
                $("record_count").sum().as("total")
            );

        // 4. 执行计算，打印控制台
        resultTable.execute().print();
    }

}
