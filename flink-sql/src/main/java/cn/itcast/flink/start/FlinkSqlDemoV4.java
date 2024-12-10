package cn.itcast.flink.start;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * 基于Flink SQL实现数据离线分析：加载文件系统中文本文件数据（tsv格式数据），封装到Table表中，进行数据分析处理
 * @author xuanyu
 */
public class FlinkSqlDemoV4 {

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
        /*
            分析电影数据，获取最受欢迎10部电影：Top10电影
         */
        TableResult tableResult = tableEnv.executeSql(
        "WITH tmp AS (\n" +
            "  SELECT movie_id, COUNT(movie_id) AS rating_people,  ROUND(AVG(rating), 2) AS rating_number\n" +
            "  FROM tbl_ratings \n" +
            "  GROUP BY movie_id\n" +
            ") \n" +
            "SELECT * FROM tmp WHERE rating_people > 400 ORDER BY rating_number DESC, rating_people DESC LIMIT 10\n"
        );

        // 4. 将数据打印控制台
        tableResult.print();
    }

}
