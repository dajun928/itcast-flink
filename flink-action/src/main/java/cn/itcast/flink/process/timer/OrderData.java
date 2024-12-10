package cn.itcast.flink.process.timer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 交易订单数据封装实体类
 * @author xuyuan
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class OrderData{
	private String orderId;
	private String userId;
	private String orderTime;
	private String orderStatus ;
	private Double orderAmount ;
}