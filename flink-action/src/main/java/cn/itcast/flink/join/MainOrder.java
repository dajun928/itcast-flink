package cn.itcast.flink.join;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 订单数据实体类
 * @author xuyuan
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MainOrder {

    /**
        数据：2022-04-05 06:00:12,order_103,user_3,shanghai-changtai,45.00
     */
    private String orderTime;
    private String orderId;
    private String userId;
    private String address;
    private Double orderMoney;

    @Override
    public String toString() {
        return orderTime + "," + orderId + "," + userId + "," + address + "," + orderMoney;
    }
}
