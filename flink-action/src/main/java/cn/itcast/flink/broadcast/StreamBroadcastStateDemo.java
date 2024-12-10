package cn.itcast.flink.broadcast;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 实时处理大表的数据，与小表数据进行关联，其中小表数据动态变化（比如新用户增加，老用户更新等）
 * 		大表数据：流式数据，存储kafka消息队列，此处演示自定义数据源产生日志流数据
 * 		小表数据：动态数据，存储MySQL数据库中，自定义数据源，实时全量加载表中数据
 * @author xuyuan
 */
public class StreamBroadcastStateDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		// 2-1. 构建小表数据流：用户信息 <userId, name, age>
		DataStreamSource<UserInfo> userStream = env.addSource(new UserInfoSource());
		userStream.print("user>");

		// 2-2. 构建大表数据流：用户行为日志，<userId, productId, trackTime, eventType>
		DataStreamSource<TrackLog> logStream = env.addSource(new TrackLogSource());
		logStream.printToErr("log>");

		// 3. 数据转换-transformation
		// todo 3-1. 广播小表数据流
		/*
			将小表数据流广播以后，存储MapState中，Key -> 关联字段：userId, Value -> 小表中数据，用户信息数据
		 */
		MapStateDescriptor<String, UserInfo> descriptor = new MapStateDescriptor<String, UserInfo>(
			"userInfoState", String.class, UserInfo.class
		);
		BroadcastStream<UserInfo> broadcastStream = userStream.broadcast(descriptor);

		// todo 3-2. 将大表数据流与广播小表数据流进行连接connect
		SingleOutputStreamOperator<String> processStream = logStream
			.connect(broadcastStream)
			.process(
				new BroadcastProcessFunction<TrackLog, UserInfo, String>() {

					// 处理大表数据流中每条数据，todo：大数据数据流中每条数据，依据userId到广播状态中获取用户信息
					@Override
					public void processElement(TrackLog value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
						// a. 获取广播状态数据（小表数据）
						ReadOnlyBroadcastState<String, UserInfo> broadcastState = ctx.getBroadcastState(descriptor);
						// b. 依据日志数据中userId获取用户信息
						String userId = value.getUserId();
						if(broadcastState.contains(userId)){
							UserInfo userInfo = broadcastState.get(userId);
							// 输出关联数据
							String output = userInfo.toString() + " -> " + value.toString();
							out.collect(output);
						}else{
							String output = "unknown -> " + value.toString();
							out.collect(output);
						}
					}

					// 处理广播流中每条数据，todo: 广播流中每条数据放入到广播状态中
					@Override
					public void processBroadcastElement(UserInfo value, Context ctx, Collector<String> out) throws Exception {
						// a. 获取存储广播流数据的状态
						BroadcastState<String, UserInfo> broadcastState = ctx.getBroadcastState(descriptor);
						// b. 将广播流中数据放入到状态中
						broadcastState.put(value.getUserId(), value);
					}
				}
			);

		// 4. 数据终端-sink
		processStream.printToErr();

		// 5. 触发执行-execute
		env.execute("StreamBroadcastStateDemo");
	}

}  