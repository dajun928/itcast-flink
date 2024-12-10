package cn.itcast.flink.test.oop;

/**
 * Java中创建线程Thread并且启动
 *
 * @author xuanyu
 */
public class JavaThreadTest {

    // todo: 方式一、创建接口实现子类，实现抽象方法
    static class MyThread implements Runnable {
        @Override
        public void run() {
            long counter = 1;
            while (true) {
                System.out.println(counter + "......................");
                counter++;
            }
        }
    }

    public static void main(String[] args) {

        // 1. 创建Thread类对象, 传递Runnable接口实例对象 -> public Thread(Runnable target)
        Thread thread = new Thread(new MyThread());

        // 2. 启动线程，调用start方法
        thread.start();
    }

}
