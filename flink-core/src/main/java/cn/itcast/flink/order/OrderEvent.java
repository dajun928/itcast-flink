package cn.itcast.flink.order;

import lombok.*;

/**
 * 交易订单数据封装实体类
 * @author xuyuan
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderEvent {

	private String orderId;
	private String userId;
	private Double orderMoney;
	private String orderTime;

	@Override
	public String toString() {
		return orderTime + "," + userId + "," + orderMoney + "," + orderId;
	}
}