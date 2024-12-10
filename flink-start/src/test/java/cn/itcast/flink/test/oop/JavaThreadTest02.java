package cn.itcast.flink.test.oop;

/**
 * Java中创建线程Thread并且启动
 *
 * @author xuanyu
 */
public class JavaThreadTest02 {

    public static void main(String[] args) {

        // 1. 创建Thread类对象, 传递Runnable接口实例对象 -> public Thread(Runnable target)
        Thread thread = new Thread(
            // todo： 方式二， 采用匿名内部类，创建对象
            new Runnable() {
                @Override
                public void run() {
                    long counter = 1;
                    while (true) {
                        System.out.println(counter + "......................");
                        counter++;
                    }
                }
            }
        );

        // 2. 启动线程，调用start方法
        thread.start();
    }

}
