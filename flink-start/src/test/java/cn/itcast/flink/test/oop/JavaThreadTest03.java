package cn.itcast.flink.test.oop;

/**
 * Java中创建线程Thread并且启动
 *
 * @author xuanyu
 */
public class JavaThreadTest03 {

    public static void main(String[] args) {

        // 1. 创建Thread类对象, 传递Runnable接口实例对象 -> public Thread(Runnable target)
        Thread thread = new Thread(
            // todo： 方式三， Java8中提供lambda表达式，简洁版
            () -> {
                long counter = 1;
                while (true) {
                    System.out.println(counter + "......................");
                    counter++;
                }
            }
            // 类似Python中lambda表达式 ->   lambda (x): x * x
        );

        // 2. 启动线程，调用start方法
        thread.start();
    }

}
