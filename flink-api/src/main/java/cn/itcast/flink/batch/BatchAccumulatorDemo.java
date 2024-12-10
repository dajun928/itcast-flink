package cn.itcast.flink.batch;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

/**
 * 批处理中高级特性：案例演示【Flink 中累加器Accumulator使用，统计处理数据集的条目数】
 *
 * @author xuanyu
 */
public class BatchAccumulatorDemo {

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

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 2. 数据源-source
        DataSource<String> dataset = env.readTextFile("datas/click.log");

        // 3. 数据转换-transformation
        MapOperator<String, ClickLog> mapDataSet = dataset.map(
            new RichMapFunction<String, ClickLog>() {
                // todo step1. 定义累加器
                private IntCounter counter = new IntCounter();

                @Override
                public void open(Configuration parameters) throws Exception {
                    // todo step2. 注册累加器
                    getRuntimeContext().addAccumulator("counter", counter);
                }

                @Override
                public ClickLog map(String value) throws Exception {
                    // todo step3. 使用累加器进行计数
                    counter.add(1);

                    return JSON.parseObject(value, ClickLog.class);
                }
            }
        );

        // 4. 数据终端-sink
        //mapDataSet.print();
        mapDataSet.writeAsText("datas/click.data");

        // 5. 触发执行-execute
        JobExecutionResult jobResult = env.execute("BatchAccumulatorDemo");

        // todo step4. 获取累加器的值
        Object counter = jobResult.getAccumulatorResult("counter");
        System.out.println("counter = " + counter);
    }

}  