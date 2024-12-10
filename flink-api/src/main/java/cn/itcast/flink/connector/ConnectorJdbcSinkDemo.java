package cn.itcast.flink.connector;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 将DataStream数据流中数据写入到MySQL数据库表中，使用JdbcSink接收器。
 *
 * @author xuanyu
 */
public class ConnectorJdbcSinkDemo {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<Student> inputDataStream = env.fromElements(
            new Student(34, "zhaoliu", 19),
            new Student(35, "wangwu", 20),
            new Student(36, "zhaoliu", 19)
        );

        // 3. 数据转换-transformation

        // 4. 数据终端-sink
        // 4-1. 创建Sink接收器对象
        SinkFunction<Student> jdbcSink = JdbcSink.sink(
            // a. 插入数据
            //"INSERT INTO db_flink.t_student(id, name, age) VALUES (?, ?, ?)",
            "REPLACE INTO db_flink.t_student(id, name, age) VALUES (?, ?, ?)",
            // b. 设置语句中占位的值
            new JdbcStatementBuilder<Student>() {
                @Override
                public void accept(PreparedStatement pstmt, Student student) throws SQLException {
                    pstmt.setInt(1, student.id);
                    pstmt.setString(2, student.name);
                    pstmt.setInt(3, student.age);
                }
            },
            // c. 插入数据时属性设置
            JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build(),
            // d. 数据库连接属性设置
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName("com.mysql.jdbc.Driver")
                .withUrl("jdbc:mysql://node1.itcast.cn:3306/?useSSL=false")
                .withUsername("root")
                .withPassword("123456")
                .withConnectionCheckTimeoutSeconds(5000)
                .build()
        );
        // 4-2. 添加接收器
        inputDataStream.addSink(jdbcSink);

        // 5. 触发执行-execute
        env.execute("ConnectorJdbcSinkDemo");
    }

}  