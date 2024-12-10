package cn.itcast.flink.join;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 订单数据和订单详情数据join拉宽后实体类
 * @author xuyuan
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DwdOrder {

    private String orderTime;
    private String orderId;
    private String userId;
    private String address;
    private Double orderMoney;

    private String detailOrderTime;
    private String detailId;
    private String goodsName;
    private int goodsNumber;
    private double detailMoney;

}
