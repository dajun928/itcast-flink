package cn.itcast.flink.batch;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Flink 中批处理分布式缓存：将小文件数据缓存到TaskManager内存中，被Slot中运行SubTask任务所使用的。
 *
 * @author xuanyu
 */
public class BatchDistributedCacheDemo {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // todo step1. 将数据文件进行缓存，注意文件不能太大，属于小文件数据
        env.registerCachedFile("datas/distribute_cache_student", "cache_students");

        // 2. 数据源-source
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

        // 3. 数据转换-transformation
        MapOperator<Tuple3<Integer, String, Integer>, String> resultDataSet = scoreDataSet
            .map(new CacheMapFunction());

        // 4. 数据终端-sink
        resultDataSet.print();
        ;

        // 5. 触发执行-execute
        //env.execute("BatchDistributedCacheDemo");
    }

    /**
     * 定义子类，实现函数接口，对大表数据处理，使用缓存文件数据
     */
    private static class CacheMapFunction extends RichMapFunction<Tuple3<Integer, String, Integer>, String> {
        // 定义map集合，存储广播数据
        private Map<Integer, String> stuMap = new HashMap<>();

        @Override
        public void open(Configuration parameters) throws Exception {
            // todo step2. 获取分布式缓存的数据
            File file = getRuntimeContext().getDistributedCache().getFile("cache_students");

            // todo step3. 读取缓存文件数据, 使用工具类
            List<String> list = FileUtils.readLines(file, Charset.defaultCharset());
            for (String line : list) {
                String[] array = line.split(",");
                stuMap.put(Integer.valueOf(array[0]), array[1]);
            }
        }

        @Override
        public String map(Tuple3<Integer, String, Integer> value) throws Exception {
            // value -> Tuple3.of(1, "语文", 50)  ==>  "张三,语文,50"
            Integer stuId = value.f0;
            // 依据学生ID，到map集合中，获取学生年龄
            String stuName = stuMap.getOrDefault(stuId, "未知");
            // 拼凑字符串返回
            return stuName + "," + value.f1 + "," + value.f2;
        }
    }
}