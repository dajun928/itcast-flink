package cn.itcast.flink.batch;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Flink 批处理中广播变量：将小数据集广播到TaskManager内存中，便于TM中Slot内运行SubTask任务共享使用。
 *
 * @author xuanyu
 */
public class BatchBroadcastDemo {

    /**
     * 定义子类，实现函数接口：MapFunction，重新方法map，实现数据处理，使用广播变量
     */
    private static class BroadcastMapFunction
        extends RichMapFunction<Tuple3<Integer, String, Integer>, String> {
        // 定义map集合，存储广播数据
        private Map<Integer, String> stuMap = new HashMap<>();

        @Override
        public void open(Configuration parameters) throws Exception {
            // todo step2. 获取广播的数据集
            List<Tuple2<Integer, String>> list = getRuntimeContext().getBroadcastVariable("students");
            // todo step3. 将广播变量数据放到map集合中，当处理大表数据时，依据key获取表中value值
            for (Tuple2<Integer, String> item : list) {
                stuMap.put(item.f0, item.f1);
            }
        }

        @Override
        public String map(Tuple3<Integer, String, Integer> value) throws Exception {
            // value ->  Tuple3.of(1, "语文", 50)  ==>  "张三,语文,50"
            Integer stuId = value.f0;
            // 依据学生ID，到map集合中，获取学生年龄
            String stuName = stuMap.getOrDefault(stuId, "未知");
            // 拼凑字符串返回
            return stuName + "," + value.f1 + "," + value.f2;
        }

    }

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 数据源-source
        // 大表数据
        DataSource<Tuple3<Integer, String, Integer>> scoreDataSet = env.fromCollection(
            Arrays.asList(
                Tuple3.of(1, "语文", 50),
                Tuple3.of(1, "数学", 70),
                Tuple3.of(1, "英语", 86),
                Tuple3.of(2, "语文", 80),
                Tuple3.of(2, "数学", 86),
                Tuple3.of(2, "英语", 96),
                Tuple3.of(3, "语文", 90),
                Tuple3.of(3, "数学", 68),
                Tuple3.of(3, "英语", 92)
            )
        );
        // 小表数据
        DataSource<Tuple2<Integer, String>> studentDataSet = env.fromCollection(
            Arrays.asList(
                Tuple2.of(1, "张三"),
                Tuple2.of(2, "李四"),
                Tuple2.of(3, "王五")
            )
        );

        // 3. 数据转换-transformation
        /*
            使用map算子，对成绩数据集scoreDataSet中stuId转换为stuName，关联学生信息数据集studentDataSet
         */
        MapOperator<Tuple3<Integer, String, Integer>, String> resultDataSet = scoreDataSet
            .map(new BroadcastMapFunction())
            // todo: step1. 将小表数据广播出去，哪个算子使用小表，就在算子后面进行广播，必须指定名称
            .withBroadcastSet(studentDataSet, "students");

        // 4. 数据终端-sink
        resultDataSet.print();

        // 5. 触发执行-execute
        //env.execute("BatchBroadcastDemo");
    }

}  