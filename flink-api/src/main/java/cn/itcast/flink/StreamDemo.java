package cn.itcast.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 依据模块创建Flink Stream流式程序Demo类
 *
 * @author xuanyu
 */
public class StreamDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 数据源-source

        // 3. 数据转换-transformation

        // 4. 数据终端-sink

        // 5. 触发执行-execute
        env.execute("StreamDemo");
    }

}  