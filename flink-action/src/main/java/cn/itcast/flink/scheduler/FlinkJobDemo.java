package cn.itcast.flink.scheduler;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 使用Flink 计算引擎实现流式数据处理：从Socket接收数据，实时进行词频统计WordCount
 * @author xuyuan
 */
public class FlinkJobDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		Configuration configuration = new Configuration();
		configuration.setString("rest.port", "8081");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration) ;
		// todo: 执行环境级别并行度
		env.setParallelism(2) ;

		// todo 2. 数据源-source
		DataStream<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);

		// todo 3. 数据转换-transformation
		// 3.1 将每行数据按照分隔符分割为单词
		DataStream<String> wordStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String line, Collector<String> out) throws Exception {
				for (String word : line.trim().split("\\s+")) {
					out.collect(word);
				}
			}
		});

		// 3.2 转换每个单词为二元组，表示单词出现一次
		DataStream<Tuple2<String, Integer>> tupleStream = wordStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(String word) throws Exception {
				return Tuple2.of(word, 1);
			}
		});

		// 3.3 按照单词分组和组内聚合累加
		DataStream<Tuple2<String, Integer>> outputStream = tupleStream
			.keyBy(tuple -> tuple.f0)
			.sum(1);

		// todo 4. 数据输出-sink
		outputStream.printToErr();

		// 5. 执行应用-execute
		env.execute("FlinkJobDemo");
	}

}