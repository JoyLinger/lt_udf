package io.transwarp.udf_back;


import io.transwarp.dao.ConnectionPool;
import io.transwarp.dao.LocalBaseDao;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by zaish on 2016-11-3.
 */
public class GetLacInfo_hyperdriver extends UDF {
  private static ConnectionPool connPool;
  private static Connection conn;
  private static PreparedStatement prest = null;
  private static ResultSet rs = null;
  private static Logger logger = Logger.getLogger(GetLacInfo_hyperdriver.class);

  static {
    try {
      connPool = LocalBaseDao.getConnectionPool();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * 输入：code与标志位
   * 输出：返回省份_城市_经度_纬度
   */
  public static String evaluate(String lac, String cellid) {
    try {
      conn = connPool.getConnection();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    String sql = "";
    String res = "";
    sql = "select PROVINCE_NAME,CITY_NAME,LONGITUDE_BUILD,LATITUDE_BUILD from hb_lac_info where key.lac=? and key.cellid=?";
    try {
      prest = conn.prepareStatement(sql);
      prest.setString(1, lac);
      prest.setString(2, cellid);
      rs = prest.executeQuery();
      if (rs.next()) {
        res = rs.getString(1) + "_" + rs.getString(2) + "_" + rs.getString(3) + "_" + rs.getString(4);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      LocalBaseDao.releaseResource(rs, prest, null);
      connPool.returnConnection(conn);
    }
    return res;
  }
}
