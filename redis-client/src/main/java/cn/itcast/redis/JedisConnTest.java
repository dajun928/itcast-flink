package cn.itcast.redis;

import redis.clients.jedis.Jedis;

/**
 * 创建Jedis对象，测试连接Redis数据库服务是否成功
 *
 * @author xuanyu
 */
public class JedisConnTest {

    public static void main(String[] args) {
        // todo step1. 创建Jedis连接对象，指定Redis服务器地址和端口号
        Jedis jedis = new Jedis("node1.itcast.cn", 6379);
        // 选择数据库, 标号索引为:0, 表示选择第1个数据库
        jedis.select(0);

        // todo step2. 测试连接
        String returnValue = jedis.ping();
        // 如果连接服务器成功，返货pong
        System.out.println(returnValue);

        // todo step3. 关闭连接
        jedis.close();
    }

}
