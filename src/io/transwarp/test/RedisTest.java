package io.transwarp.test;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisTest {
  private Jedis jedis;
  private JedisPool jedisPool;
  private String ip = "10.1.100.221";

  public RedisTest() {
    initialPool();
    jedis = jedisPool.getResource();
    jedis.auth("foobared");
  }

  private void initialPool() {
    JedisPoolConfig config = new JedisPoolConfig();
    config.setMaxTotal(20);
    config.setMaxIdle(5);
    config.setMaxWaitMillis(10000);
    config.setTestOnBorrow(false);
    jedisPool = new JedisPool(config, ip, 6379);
  }

  public void show() {
    testHash();
  }

  private void testHash() {
    System.out.println("=============hash==========================");
// 清空数据
    System.out.println(jedis.flushDB());
// 添加数据
    jedis.hset("hashs", "entryKey", "entryValue");
    jedis.hset("hashs", "entryKey1", "entryValue1");
    jedis.hset("hashs", "entryKey2", "entryValue2");
// 判断某个值是否存在
    System.out.println(jedis.hexists("hashs", "entryKey"));
// 获取指定的值
    System.out.println(jedis.hget("hashs", "entryKey"));
// 批量获取指定的值
    System.out
            .println(jedis.hmget("hashs", "entryKey", "entryKey1"));
// 删除指定的值
    System.out.println(jedis.hdel("hashs", "entryKey"));
// 为key中的域 field 的值加上增量 increment
    System.out.println(jedis.hincrBy("hashs", "entryKey", 123l));
// 获取所有的keys
    System.out.println(jedis.hkeys("hashs"));
// 获取所有的values
    System.out.println(jedis.hvals("hashs"));
    jedis.close();
    jedisPool.close();
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    new RedisTest().show();
  }
}
