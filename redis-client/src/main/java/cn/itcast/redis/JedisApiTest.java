package cn.itcast.redis;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Jedis API 操作Redis数据库中数据，进行CRUD操作，针对不同Value数据类型操作
 *
 * @author xuanyu
 */
public class JedisApiTest {

    /**
     * 定义Jedis变量
     */
    private Jedis jedis = null;

    /**
     * 初始化操作
     */
    @Before
    public void open() {
        // todo step1. 创建Jedis连接对象，指定Redis服务器地址和端口号
        jedis = new Jedis("node1.itcast.cn", 6379);
        // 选择数据库, 标号索引为:0, 表示选择第1个数据库
        jedis.select(0);
    }

    /**
     * 测试Jedis连接Redis数据库是否成功
     */
    @Test
    public void testJedis() {
        String returnValue = jedis.ping();
        System.out.println(returnValue);
    }

    /**
     * 测试Jedis操作string类型数据：set\get\exists\expire\ttl\del\
     */
    @Test
    public void testString() {
        // 设置数据（写入数据）
        jedis.set("name", "Tom");
        // 读取数据
        System.out.println(jedis.get("name"));

        // 判断key是否存在
        System.out.println("name is exists: " + jedis.exists("name"));
        System.out.println("age is exists: " + jedis.exists("age"));

        // 设置key有效期
        jedis.set("number", "100");
        jedis.expire("number", 20);
        System.out.println("number ttl: " + jedis.ttl("number"));

        // 删除一个key
        jedis.del("name");
        System.out.println("name is exists: " + jedis.exists("name"));
    }

    /**
     * 测试Jedis操作hash类型数据：hset\hget\hdel\hgetall
     */
    @Test
    public void testHash() {
        // 放入数据，指定key、字段名称和字段值
        jedis.hset("user:1", "name", "张三疯");
        jedis.hset("user:1", "age", "145");
        jedis.hset("user:1", "gender", "male");
        jedis.hset("user:1", "address", "武当山");

        // 获取某个字段的值
        String nameValue = jedis.hget("user:1", "name");
        System.out.println("name = " + nameValue);

        // 判断某个字段是否存在
        Boolean isFlag = jedis.hexists("user:1", "telphone");
        System.out.println("telphone: " + isFlag);

        // 获取hash所有字段和值
        Map<String, String> valueMap = jedis.hgetAll("user:1");
        System.out.println(valueMap);

        // 删除某个字段的值
        Long deleteValue = jedis.hdel("user:1", "gender");
        System.out.println("delete: " + deleteValue);
    }

    @Test
    public void testSet() {
        //jedis.sadd()

        //jedis.sismember()

        //jedis.scard()

        //jedis.smembers()
    }

    /**
     * 收尾工作
     */
    @After
    public void close() {
        // todo step3. 关闭连接
        if (null != jedis) {
            jedis.close();
        }
    }

}
