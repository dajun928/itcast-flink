package cn.itcast.flink.transformation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 案例演示：自定义Sink数据接收器，将数据流DataStream保存到MySQL表中，实现抽象类RichSinkFunction
 *
 * @author xuanyu
 */
public class TransformationRichDemo {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }


    /**
     * 自定义Sink接收器，将DataStream中数据写入到MySQL数据库表中，采用Jdbc方式写入数据
     */
    private static class MysqlSink extends RichSinkFunction<Student> {

        // 定义变量
        private Connection connection = null;
        private PreparedStatement pstmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("调用open()方法，初始化操作：创建数据库连接...................................................");

            // 获取Job运行时上下文对象
            // RuntimeContext runtimeContext = getRuntimeContext();

            // step1. 加载驱动
            Class.forName("com.mysql.jdbc.Driver");
            // step2. 获取连接
            connection = DriverManager.getConnection(
                "jdbc:mysql://node1.itcast.cn:3306/?useSSL=false", "root", "123456"
            );
            // step3. 创建Statement对象
            pstmt = connection.prepareStatement(
                "INSERT INTO db_flink.t_student(id, name, age) VALUES (?, ?, ?)"
            );
        }

        /**
         * todo: 数据流中每条数据进行输出操作，调用invoke方法
         */
        @Override
        public void invoke(Student student, Context context) throws Exception {
            System.out.println("调用invoke()方法，写入数据到数据库表中...................................................");

            // step4. 执行操作
            pstmt.setInt(1, student.id);
            pstmt.setString(2, student.name);
            pstmt.setInt(3, student.age);
            pstmt.execute();
        }

        @Override
        public void close() throws Exception {
            System.out.println("调用close()方法，收尾工作：关闭连接...................................................");

            // step5. 关闭连接
            if (null != pstmt && !pstmt.isClosed()) {
                pstmt.close();
            }
            if (null != connection && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<Student> inputDataStream = env.fromElements(
            new Student(31, "wangwu", 20),
            new Student(32, "zhaoliu", 22),
            new Student(33, "laoda", 25),
            new Student(34, "laoer", 23),
            new Student(35, "laosan", 21)
        );

        // 3. 数据转换-transformation

        // 4. 数据终端-sink
        MysqlSink mysqlSink = new MysqlSink();
        inputDataStream.addSink(mysqlSink);

        // 5. 触发执行-execute
        env.execute("StreamSinkMysqlDemo");
    }

}