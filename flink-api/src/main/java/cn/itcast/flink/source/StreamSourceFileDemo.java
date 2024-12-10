package cn.itcast.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 流计算数据源：基于文件数的Source，可以是压缩文件，必须时支持的压缩格式，自动依据文件后缀名判断压缩进行解压读取数据
 *
 * @author xuanyu
 */
public class StreamSourceFileDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        // 方式一：读取文本文件
        DataStreamSource<String> dataStream01 = env.readTextFile("datas/words.txt");
        dataStream01.printToErr();

        // 方式二：读取压缩文件
        DataStreamSource<String> dataStream02 = env.readTextFile("datas/words.txt.gz");
        dataStream02.print("gz");

        // 3. 数据转换-transformation

        // 4. 数据终端-sink

        // 5. 触发执行-execute
        env.execute("StreamSourceFileDemo");
    }

}  