package cn.itcast.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 使用Flink计算引擎实现离线批处理：词频统计WordCount，从本地文件系统读取文本文件，结果打印到控制台或保存文件。
 * 1. 执行环境-env
 * 2. 数据源-source
 * 3. 数据转换-transformation
 * 4. 数据接收器-sink
 * 5. 触发执行-execute
 *
 * @author xuanyu
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 数据源-source
        // todo: readTextFile 加载文本文件中数据时，一行一行读取数据
        DataSource<String> inputDataSet = env.readTextFile("datas/words.txt");

        // 3. 数据转换-transformation
        /*
                        spark hive spark
                                |flatMap
            分割单词：     spark,  hive,   spark
                                |map
            转换二元组：   (spark, 1),  (hive, 1),  (spark, 1)    todo: Flink Java API中提供元组封装类型Tuple1/Tuple2/...
                                |groupBy(0)   todo: 0属于元组下标，表示元组中第一个元素
            分组：      spark ->  [(spark, 1),  (spark, 1) ]          hive  ->  [(hive, 1)]
                                |sum(1)  todo: 1属于元组下标，表示元组中第二个元素
            求和：         spark ->  1 + 1 = 2     ，                 hive -> 1 = 1
         */
        // 3-1. 分割单词 todo: rdd.flatMap(lambda value: value.split(' '))
        FlatMapOperator<String, String> wordDataSet = inputDataSet.flatMap(
            // 采用匿名内部类方式，创建函数接口对象： new 接口名称(){  // 实现接口中抽象方法 }
            new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String value, Collector<String> out) throws Exception {
                    String[] words = value.split("\\s+");
                    for (String word : words) {
                        out.collect(word);
                    }
                }
            }
        );

        // 3-2. 转换二元组 todo: word_rdd.map(lambda word: (word, 1))
        MapOperator<String, Tuple2<String, Integer>> tupleDataSet = wordDataSet.map(
            new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String value) throws Exception {
                    return Tuple2.of(value, 1);
                }
            }
        );

        // 3-3. 按照单词分组并且组内求和 todo: tuple_rdd.reduceByKey(lambda tmp, item: tmp + item)
        AggregateOperator<Tuple2<String, Integer>> resultDataSet = tupleDataSet.groupBy(0).sum(1);

        // 4. 数据接收器-sink
        resultDataSet.print();
        /*
            (hdfs,1)
            (hadoop,2)
            (hive,4)
            (python,3)
            (mapreduce,2)
            (spark,7)
         */
        resultDataSet.writeAsText("datas/result-wc");

        // 5. 触发执行-execute
        env.execute("BatchWordCount");
    }

}
