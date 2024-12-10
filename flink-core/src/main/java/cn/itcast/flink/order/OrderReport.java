package cn.itcast.flink.order;

import lombok.*;

/**
 * 窗口计算结果字段封装实体类，包含字段：userId, totalMoney, windowStart, windowEnd
 * @author xuyuan
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderReport {
	private String userId;
	private String windowStart;
	private String windowEnd;
	private Double totalMoney ;

	@Override
	public String toString() {
		return "[" + windowStart + " ~ " + windowEnd + "]: " + userId + " = " + totalMoney;
	}
}