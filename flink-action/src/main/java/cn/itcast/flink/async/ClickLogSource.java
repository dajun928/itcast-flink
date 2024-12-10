package cn.itcast.flink.async;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 自定义数据源，实时产生用户行为日志数据
 */
public class ClickLogSource extends RichSourceFunction<String> {
	private boolean isRunning = true ;

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		String[] array = new String[]{"click", "browser", "browser", "click", "browser", "browser", "search"};
		Random random = new Random();
		FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS") ;
		// 模拟用户点击日志流数据
		while (isRunning){
			String userId = "u_" + (1000 + random.nextInt(10)) ;
			String behavior = array[random.nextInt(array.length)] ;
			Long timestamp = System.currentTimeMillis();

			String output = userId + "," + behavior + "," + format.format(timestamp) ;
			// 输出
			ctx.collect(output);
			// 每隔至少1秒产生1条数据
			TimeUnit.SECONDS.sleep( 1 + random.nextInt(2));
		}
	}

	@Override
	public void cancel() {
		isRunning = false ;
	}
}