package cn.itcast.flink.join;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 订单详情实体类
 * @author xuyuan
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DetailOrder {
    /**
        数据：2022-04-05 06:00:12,order_103,detail_1,milk,1,45.00
     */
    private String detailTime;
    private String orderId;
    private String detailId;
    private String goodsName;
    private int goodsNumber;
    private double detailMoney;

    @Override
    public String toString() {
        return detailTime + "," + orderId + "," + detailId + "," + goodsName + "," + goodsNumber + "," + detailMoney;
    }
}
