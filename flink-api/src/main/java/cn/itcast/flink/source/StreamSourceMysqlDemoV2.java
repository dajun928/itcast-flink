package cn.itcast.flink.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

/**
 * 从MySQL中实时加载数据：从MySQL中实时加载数据，要求MySQL中的数据有变化，也能被实时加载出来
 *
 * @author xuanyu
 */
public class StreamSourceMysqlDemoV2 {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }

    /**
     * 自定义数据源，加载Mysql数据库表中的数据，每隔5秒中加载1次表中数据
     */
    private static class MysqlSource extends RichParallelSourceFunction<Student> {
        // 定义变量，作为标识符，是否加载数据
        private boolean isRunning = true;

        // 定义变量
        private Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet result = null;

        /**
         * todo: 初始化方法，在调用run方法之前，调用的方法，每个实例对象，仅仅调用1次，比如获取连接
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            // step1. 加载驱动
            Class.forName("com.mysql.jdbc.Driver");
            // step2. 获取连接
            connection = DriverManager.getConnection(
                "jdbc:mysql://node1.itcast.cn:3306/?useSSL=false", "root", "123456"
            );
            // step3. 创建Statement对象
            pstmt = connection.prepareStatement("SELECT id, name, age FROM db_flink.t_student");
        }

        @Override
        public void run(SourceContext<Student> ctx) throws Exception {
            while (isRunning) {
                /*
                    todo 从Mysql数据库加载数据，使用jdbc方式读取数据
                 */
                // step4. 执行操作
                result = pstmt.executeQuery();
                // step5. 获取数据
                while (result.next()) {
                    // 获取每条数据
                    int id = result.getInt("id");
                    String name = result.getString("name");
                    int age = result.getInt("age");
                    // todo: 封装数据到实体类对象中，发送数据到下游
                    Student student = new Student(id, name, age);
                    ctx.collect(student);
                }

                // todo 每隔5秒钟加载一次数据
                TimeUnit.SECONDS.sleep(5);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        /**
         * todo: 收尾工作方法，每个实例对象调用1次，比如关闭资源
         */
        @Override
        public void close() throws Exception {
            // step6. 关闭连接
            if (null != result) {
                result.close();
            }
            if (null != pstmt) {
                pstmt.close();
            }
            if (null != connection) {
                connection.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        MysqlSource mysqlSource = new MysqlSource();
        DataStreamSource<Student> studentDataStream = env.addSource(mysqlSource);

        // 3. 数据转换-transformation

        // 4. 数据终端-sink
        studentDataStream.print();

        // 5. 触发执行-execute
        env.execute("StreamSourceMysqlDemo");
    }

}