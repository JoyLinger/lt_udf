package io.transwarp.udf_back;

import io.transwarp.dao.ConnectionPool;
import io.transwarp.dao.LocalBaseDao;
import io.transwarp.utils.LoadData;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;


/**
 * Created by zaish on 2016-11-3.
 */
public class GetAppInfo_hyperdrive extends UDF {
  private static Map appBigTypeMap;
  private static Map appSmallTypeMap;
  private static Map dataTypeMap;
  private static ConnectionPool connPool;
  private static Connection conn;
  private static PreparedStatement prest = null;
  private static ResultSet rs = null;

  static {
    LoadData ld = LoadData.getInstance();
    appBigTypeMap = ld.getAppBigTypeMap();
    appSmallTypeMap = ld.getAppSmallTypeMap();
    dataTypeMap = ld.getDataTypeMap();
    try {
      connPool = LocalBaseDao.getConnectionPool();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * 输入：code与标志位
   * 输出：flag=1 返回流量类型;  flag=2 返回应用大类; flag=3 返回应用小类 ;
   */
  public static String evaluate(String str, int flag) {
    try {
      conn = connPool.getConnection();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    String sql;
    String res = "";
    try {
      if (flag == 1) {
        if (dataTypeMap.containsKey(str)) {
          res = dataTypeMap.get(str).toString();
        }
      } else if (flag == 2) {
        if (appBigTypeMap.containsKey(str)) {
          res = appBigTypeMap.get(str).toString();
        }
      } else if (flag == 4) {
        sql = "select W3_SP_NAME from hb_url_app where host_hashcode=?";
        prest = conn.prepareStatement(sql);
        prest.setString(1, str);
        rs = prest.executeQuery();
        if (rs.next()) {
          res = rs.getString(1);
        }
      } else {
        throw new Exception("this flag is not supported");
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      LocalBaseDao.releaseResource(rs, prest, null);
      connPool.returnConnection(conn);
    }

    return res;
  }

  public static String evaluate(String bigtype_code, String smalltype_code, int flag) throws Exception {
    String res = "";
    if (flag == 3) {
      if (appSmallTypeMap.containsKey(bigtype_code + "_" + smalltype_code)) {
        res = appSmallTypeMap.get(bigtype_code + "_" + smalltype_code).toString();
      }
    } else {
      throw new Exception("this flag is not supported");
    }
    return res;
  }
}
