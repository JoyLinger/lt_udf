package io.transwarp.dao;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by zaish on 2016-12-12.
 */
public class JedisPoolManager {
  private volatile static JedisPoolManager manager;
  private final JedisPool pool;

  private JedisPoolManager() {
    Properties pro = new Properties();
    try {
      //加载redis配置
      pro.load(JedisPoolManager.class.getClassLoader().getResourceAsStream("jedis.properties"));
      // 创建jedis池配置实例
      JedisPoolConfig config = new JedisPoolConfig();

      // 设置池配置项值
      String maxTotal = pro.getProperty("redis.pool.maxTotal", "4");
      config.setMaxTotal(Integer.parseInt(maxTotal));

      String maxIdle = pro.getProperty("redis.pool.maxIdle", "4");
      config.setMaxIdle(Integer.parseInt(maxIdle));

      String minIdle = pro.getProperty("redis.pool.minIdle", "1");
      config.setMinIdle(Integer.parseInt(minIdle));

      String maxWaitMillis = pro.getProperty("redis.pool.maxWaitMillis", "1024");
      config.setMaxWaitMillis(Long.parseLong(maxWaitMillis));

      String testOnBorrow = pro.getProperty("redis.pool.testOnBorrow", "true");
      config.setTestOnBorrow("true".equals(testOnBorrow));

      String testOnReturn = pro.getProperty("redis.pool.testOnReturn", "true");
      config.setTestOnReturn("true".equals(testOnReturn));

      int TIME_OUT = Integer.parseInt(pro.getProperty("redis.pool.time_out", "2000"));

      String server = pro.getProperty("redis.server");
      if (StringUtils.isEmpty(server)) {
        throw new IllegalArgumentException("JedisPool redis.server is empty!");
      }

      String[] host_arr = server.split(",");
      if (host_arr.length > 1) {
        throw new IllegalArgumentException("JedisPool redis.server length > 1");
      }

      String[] arr = host_arr[0].split(":");

      // 根据配置实例化jedis池
      System.out.println("***********init JedisPool***********");
      System.out.println("host->" + arr[0] + ",port->" + arr[1]);
      pool = new JedisPool(config, arr[0], Integer.parseInt(arr[1]), TIME_OUT);

    } catch (IOException e) {
      throw new IllegalArgumentException("init JedisPool error", e);
    }

  }

  public static JedisPoolManager getMgr() {
    if (manager == null) {
      synchronized (JedisPoolManager.class) {
        if (manager == null) {
          manager = new JedisPoolManager();
        }
      }
    }
    return manager;
  }

  public synchronized Jedis getResource() {
    return pool.getResource();
  }

  public void destroy() {
    // when closing your application:
    pool.destroy();
  }

  public void close() {
    pool.close();
  }

}
