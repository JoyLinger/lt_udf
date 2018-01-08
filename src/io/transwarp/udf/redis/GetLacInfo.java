package io.transwarp.udf.redis;


import io.transwarp.dao.JedisPoolManager;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Created by zaish on 2016-11-3.
 */
public class GetLacInfo extends UDF {
  private static JedisPoolManager jedisPoolManager;
  private static Jedis jedis;
  private static Logger LOGGER = Logger.getLogger(GetLacInfo.class);

  static {
    try {
      jedisPoolManager = JedisPoolManager.getMgr();
    } catch (Exception e) {
      LOGGER.error("Error msg is: " + e.getMessage());
//            e.printStackTrace();
    }
  }

  /*
   * 输入：code与标志位
   * 输出：返回省份_城市_经度_纬度
   */
  public static String evaluate(String lac, String cellid) {
    jedis = jedisPoolManager.getResource();
    jedis.auth("foobared");
    String key = "lac_info:" + lac + "_" + cellid;
    StringBuilder res = new StringBuilder();
    List<String> list = jedis.hmget(key, "province_name", "city_name", "longitude_build", "latitude_build");
    res.append(list.get(0)).append("_").append(list.get(1)).append("_").append(list.get(2)).append("_").append(list.get(3));
    jedis.close();
    return res.toString();
  }
}
