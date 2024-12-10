package cn.itcast.flink.async;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 定义子类，实现函数接口：AsyncFunction，重写asyncInvoke方法，实现异步请求数据库，获取数据
 * @author xuanyu
 */
public class AsyncMysqlRequest extends RichAsyncFunction<Tuple2<String, String>, String> {

    // 定义变量
    private Connection connection = null ;
    private PreparedStatement pstmt = null ;
    private ResultSet result = null ;

    // 定义线程池变量
    private ExecutorService executorService = null ;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化线程池
        executorService = Executors.newFixedThreadPool(10) ;

        // a. 加载驱动类
        Class.forName("com.mysql.jdbc.Driver") ;
        // b. 获取连接
        connection = DriverManager.getConnection(
            "jdbc:mysql://node1.itcast.cn:3306/?useSSL=false", "root", "123456"
        );
        // c. 实例化Statement对象
        pstmt = connection.prepareStatement("SELECT user_name FROM db_flink.tbl_user_info WHERE user_id = ?") ;
    }

    @Override
    public void asyncInvoke(Tuple2<String, String> input, ResultFuture<String> resultFuture) throws Exception {
        /*
            input -> 数据流中每条数据： (u_1009,     u_1009,click,2022-08-06 19:30:55.347)
                        |
                   wujiu,u_1009,click,2022-08-06 19:30:55.347
         */
        // 获取用户id
        String userId = input.f0 ;

        // todo: 通过线程池请求Mysql数据库，到达异步请求效果
        Future<String> future = executorService.submit(
            new Callable<String>() {
                @Override
                public String call() throws Exception {
                    // d. 设置占位符值，进行查询
                    pstmt.setString(1, userId);
                    result = pstmt.executeQuery();

                    // e. 获取查询的结果
                    String userName = "未知";
                    while (result.next()) {
                        userName = result.getString("user_name");
                    }
                    // 返回请求数据
                    return userName;
                }
            }
        );

        // 获取异步请求结果
        String userName = future.get();
        String output = userName + "," + input.f1 ;

        // 将查询数据结构异步返回
        resultFuture.complete(Collections.singletonList(output));
    }

    @Override
    public void timeout(Tuple2<String, String> input, ResultFuture<String> resultFuture) throws Exception {
        // 获取日志数据
        String log = input.f1;
        // 输出数据
        String output = "unknown," + log ;
        // 返回
        resultFuture.complete(Collections.singletonList(output));
    }


    @Override
    public void close() throws Exception {
        // 关闭线程池
        if(null != executorService) {
            executorService.shutdown();
        }

        // f. 关闭连接
        if(null != result){
            result.close();
        }
        if(null != pstmt){
            pstmt.close();
        }
        if(null != connection) {
            connection.close();
        }
    }
}
