package cn.itcast.flink.process.timer;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 自定义数据源Source，实时产生交易订单数据：orderId,userId,orderTime,orderStatus,orderAmount
 * @author xuyuan 
 */
public class OrderSource extends RichParallelSourceFunction<String> {
	String[] allStatus = new String[]{"未付款", "已付款", "已付款", "已付款", "已付款"};
	private boolean isRunning = true ;
	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		Random random = new Random();
		FastDateFormat format = FastDateFormat.getInstance("yyyyMMddHHmmssSSS");
		FastDateFormat format2 = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");
		while (isRunning){
			long currentTimeMillis = System.currentTimeMillis();
			String orderId = format.format(currentTimeMillis) + "" + (10000 + random.nextInt(10000)) ;
			String userId = (random.nextInt(5) + 1) * 100000 + random.nextInt(100000) + "" ;
			String orderTime = format2.format(currentTimeMillis);
			String orderStatus = allStatus[random.nextInt(allStatus.length)] ;
			Double orderAmount = new BigDecimal(random.nextDouble() * 100).setScale(2, RoundingMode.HALF_UP).doubleValue();

			// 输出字符串
			String output = orderId + "," + userId + "," + orderTime + "," + orderStatus + "," + orderAmount;
			ctx.collect(output);

			TimeUnit.MILLISECONDS.sleep(2000);
		}
	}
	@Override
	public void cancel() {
		isRunning = false ;
	}
}