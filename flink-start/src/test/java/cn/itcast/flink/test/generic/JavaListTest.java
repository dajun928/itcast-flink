package cn.itcast.flink.test.generic;

import java.util.ArrayList;
import java.util.List;

/**
 * Java中泛型，比如集合类接口，几乎都是泛型，存储数据类型未知
 *
 * @author xuanyu
 */
public class JavaListTest {

    public static void main(String[] args) {
        // 创建列表对象
        List<String> list = new ArrayList<String>();

        // 添加数据到列表中
        list.add("hello");
        list.add("world");
        list.add("flink");
        list.add("spark");
        //list.add(100) ;

        // 对应数据
        System.out.println(list);
    }

}
