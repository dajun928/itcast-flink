package cn.itcast.flink.transformation;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink中流计算DataStream转换算子：map\flatMap\filter
 *
 * @author xuanyu
 */
public class TransformationBasicDemo {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class ClickLog {
        //频道ID
        private long channelId;
        //产品的类别ID
        private long categoryId;

        //产品ID
        private long produceId;
        //用户的ID
        private long userId;
        //国家
        private String country;
        //省份
        private String province;
        //城市
        private String city;
        //网络方式
        private String network;
        //来源方式
        private String source;
        //浏览器类型
        private String browserType;
        //进入网站时间
        private Long entryTime;
        //离开网站时间
        private Long leaveTime;
    }

    /**
     * 定义子类，实现函数接口：MapFunction，重写方法 -> 对流中每条数据处理
     */
    private static class ParseJsonMapFunction implements MapFunction<String, ClickLog> {

        @Override
        public ClickLog map(String value) throws Exception {
            // 参数value表示数据流DataStream中每条数据
            return JSON.parseObject(value, ClickLog.class);
        }

    }

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        DataStreamSource<String> inputDataStream = env.readTextFile("datas/click.log");
        // inputDataStream.printToErr();

        // 3. 数据转换-transformation

        // TODO: 2022/7/19 [map 算子] -> 将JSON字符串转换对应JavaBean实体类对象
        //SingleOutputStreamOperator<ClickLog> mapDataStream = inputDataStream.map(new ParseJsonMapFunction());
        //mapDataStream.printToErr();

        SingleOutputStreamOperator<ClickLog> mapDataStream = inputDataStream.map(
            new MapFunction<String, ClickLog>() {
                @Override
                public ClickLog map(String value) throws Exception {
                    // 使用阿里巴巴库：fastJson库
                    return JSON.parseObject(value, ClickLog.class);
                }
            }
        );
        // mapDataStream.print();


        // TODO: 2022/7/19 [flatMap 算子】 -> 每条数据中Long类型值的时间字段转换为不同日期时间格式字符串
        /*
            Long类型日期时间：	1577890860000
                            |
                            |进行格式
                            |
            String类型日期格式
                    yyyy-MM-dd-HH
                    yyyy-MM-dd
                    yyyy-MM
            todo 使用工具类：
                DateFormatUtils, 在commons.langs 包下面
         */
        SingleOutputStreamOperator<String> flatDataStream = mapDataStream.flatMap(
            new FlatMapFunction<ClickLog, String>() {
                @Override
                public void flatMap(ClickLog clickLog, Collector<String> out) throws Exception {
                    // 获取 进入网址 时间
                    Long entryTime = clickLog.getEntryTime();

                    // 转换1： yyyy-MM-dd-HH
                    String hourValue = DateFormatUtils.format(entryTime, "yyyy-MM-dd-HH");
                    out.collect(hourValue);

                    // 转换2： yyyy-MM-dd
                    String dayValue = DateFormatUtils.format(entryTime, "yyyy-MM-dd");
                    out.collect(dayValue);

                    // 转换1： yyyy-MM
                    String monthValue = DateFormatUtils.format(entryTime, "yyyy-MM");
                    out.collect(monthValue);
                }
            }
        );
        // flatDataStream.print();


        // TODO: 2022/7/19 [filter 算子]， 过滤获取使用谷歌浏览器访问日志数据
        /*
        SingleOutputStreamOperator<ClickLog> filterDataStream = mapDataStream.filter(
                new FilterFunction<ClickLog>() {
                    @Override
                    public boolean filter(ClickLog clickLog) throws Exception {
                        return "谷歌浏览器".equals(clickLog.browserType);
                    }
                }
        );
        filterDataStream.print();
        */

        // todo 使用lambda表达式书写代码，由于接口实现类中方法体实现代码简单
        SingleOutputStreamOperator<ClickLog> googleDataStream = mapDataStream
            .filter(clickLog -> "谷歌浏览器".equals(clickLog.browserType));
        googleDataStream.printToErr();


        // 4. 数据终端-sink

        // 5. 触发执行-execute
        env.execute("TransformationBasicDemo");
    }

}  