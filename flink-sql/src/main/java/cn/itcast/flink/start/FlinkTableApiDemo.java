package cn.itcast.flink.start;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import static org.apache.flink.table.api.Expressions.*;

/**
 * 基于Flink Table Api实现批处理：加载文本文件数据，进行电影评分统计分析
 * @author xuanyu
 */
public class FlinkTableApiDemo {

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

        // 3. 查询表的数据，todo: Table Api 链式编程
        Table resultTable = tableEnv
            // a. 执行表，加载数据
            .from("tbl_ratings")
            // b. 指定分组字段，调用静态方法$ -> 将字符串转换为表达式对象
            .groupBy($("movie_id"))
            // c. 选择字段和使用聚合函数对数据聚合操作
            .select(
                $("movie_id"),
                $("movie_id").count().as("rating_people"),
                $("rating").avg().round(2).as("rating_number")
            )
            // d. 按照评分人数过滤
            .where(
                $("rating_people").isGreater(400)
            )
            // e. 设置排序字段和排序规则，可以设置多个字段
            .orderBy(
                $("rating_number").desc(), $("rating_people").desc()
            )
            // f. 获取前10条数据
            .limit(10);

        // 4. 将结果数据打印到控制台
        resultTable.execute().print();
    }

}
