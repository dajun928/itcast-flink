package cn.itcast.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 创建JedisPool连接池，获取Jedis对象，连接Redis数据库服务，进行数据操作
 *
 * @author xuanyu
 */
public class JedisPoolTest {

    public static void main(String[] args) {
        // todo step1. 创建连接池JedisPool
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        // 连接池总的连接数
        jedisPoolConfig.setMaxTotal(8);
        // 连接池中最大空闲连接数
        jedisPoolConfig.setMaxIdle(8);
        // 连接池中最小空闲连接数
        jedisPoolConfig.setMinIdle(3);
        // 传递参数，创建连接池
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, "node1.itcast.cn", 6379);

        // todo step2. 从连接池中获取连接
        Jedis jedis = jedisPool.getResource();

        // todo step3. 使用连接操作数据
        jedis.set("school", "itcast");
        System.out.println("school: " + jedis.get("school"));

        // todo step4. 关闭连接
        jedis.close();
    }

}
