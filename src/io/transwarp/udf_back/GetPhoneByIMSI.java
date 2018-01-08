package io.transwarp.udf_back;

import io.transwarp.dao.ConnectionPool;
import io.transwarp.dao.LocalBaseDao;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by zaish on 2016-11-3.
 * 输入：imsi
 * 输出：phone,若无匹配值返回""
 */
public class GetPhoneByIMSI extends UDF {
  private static ConnectionPool connPool;
  private static Connection conn;
  private static PreparedStatement prest = null;
  private static ResultSet rs = null;

  static {
    try {
      connPool = LocalBaseDao.getConnectionPool();
      conn = connPool.getConnection();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static String evaluate(String imsi) {
    String sql = "";
    String res = "";
    try {
      sql = "select phone from hb_imsi_phone where imsi=?";
      prest = conn.prepareStatement(sql);
      prest.setString(1, imsi);
      rs = prest.executeQuery();
      if (rs.next()) {
        res = rs.getString(1);
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
