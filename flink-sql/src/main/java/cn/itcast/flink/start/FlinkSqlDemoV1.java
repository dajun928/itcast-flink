package cn.itcast.flink.start;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * 基于Flink SQL实现数据离线分析：加载文件系统中文本文件数据（tsv格式数据），封装到Table表中，进行数据分析处理
 * @author xuanyu
 */
public class FlinkSqlDemoV1 {

    public static void main(String[] args) {
        // 1. 构建表的执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inBatchMode() //.inStreamingMode()
            .useBlinkPlanner()
            .build() ;
        TableEnvironment tableEnv = TableEnvironment.create(settings) ;

        // 2. 创建输入表，todo： 编写CREATE TABLE 语句，使用with语法，映射数据（文件系统中文件）到表中
        tableEnv.executeSql(
            "CREATE TABLE tbl_ratings(\n" +
                "  user_id STRING,\n" +
                "  movie_id STRING,\n" +
                "  rating DOUBLE,\n" +
                "  ts BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem', \n" +
                "  'path' = 'datas/ratings.data', \n" +
                "  'format' = 'csv',\n" +
                "  'csv.field-delimiter' = '\\t',\n" +
                "  'csv.ignore-parse-errors' = 'true'\n" +
                ")"
        );

        // 3. 查询表的数据， todo：编写SQL语句
        TableResult tableResult = tableEnv.executeSql(
            "SELECT * FROM tbl_ratings LIMIT 10"
        );

        // 4. 将数据打印控制台
        tableResult.print();

        // todo： Table API 编程
        tableEnv.from("tbl_ratings").limit(10).execute().print();
    }

}
