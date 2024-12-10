package cn.itcast.flink.async;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * 对数据流中每条数据处理时，异步请求Mysql数据库，依据字段获取数据，最终输出打印控制台。
 * @author xuyuan
 */
public class StreamAysncMysqlDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<String> dataStream = env.addSource(new ClickLogSource());
		// dataStream.printToErr();

		// 3. 数据转换-transformation
		/*
			u_1009,click,2022-08-06 19:30:55.347
					|
				请求Mysql数据库，依据userId获取用户信息，采用异步IO方式完成
		 */
		// 3-1. 将数据进行转换，封装到二元组<userId, log>
		SingleOutputStreamOperator<Tuple2<String, String>> logStream = dataStream.map(
			new MapFunction<String, Tuple2<String, String>>() {
				@Override
				public Tuple2<String, String> map(String value) throws Exception {
					// 获取用户ID
					String userId = value.split(",")[0];
					// 构建二元组并且返回
					return Tuple2.of(userId, value);
				}
			}
		);

		// todo 3-2. 异步请求Mysql数据库，采用JDB方式查询数据，由于不支持异步请求，所以使用线程池请求
		SingleOutputStreamOperator<String> asyncStream = AsyncDataStream.unorderedWait(
			logStream, // 数据流
			new AsyncMysqlRequest(), // 数据流中每条数据异步请求数据库操作
			1000, // 请求数据超时时间
			TimeUnit.MILLISECONDS, // 超时时间单位
			10
		);

		// 4. 数据终端-sink
		asyncStream.printToErr();

		// 5. 触发执行-execute
		env.execute("StreamAysncMySQLDemo");
	}

}  