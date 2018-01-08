package io.transwarp.udf.hbaseApi;

import io.transwarp.dao.ConnectionPool;
import io.transwarp.dao.LocalBaseDao;
import io.transwarp.utils.LoadData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;


/**
 * Created by zaish on 2016-11-3.
 */
public class GetAppInfo extends UDF {

  private static final Log LOGGER = LogFactory.getLog(GetAppInfo.class);
  private static Map appBigTypeMap;
  private static Map appSmallTypeMap;
  private static Map dataTypeMap;

  static {
    LoadData ld = LoadData.getInstance();
    appBigTypeMap = ld.getAppBigTypeMap();
    appSmallTypeMap = ld.getAppSmallTypeMap();
    dataTypeMap = ld.getDataTypeMap();
  }

  /*
   * 输入：code与标志位
   * 输出：flag=1 返回流量类型;  flag=2 返回应用大类; flag=3 返回应用小类 ;
   */
  public static String evaluate(String str, int flag) {
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
        res = "";
      } else {
        LOGGER.error("this flag is not supported");
      }
    } catch (Exception e) {
      LOGGER.error("Error msg is: " + e.getMessage());
    }

    return res;
  }

  public static String evaluate(String bigtype_code, String smalltype_code, int flag) {
    if (bigtype_code == null) {
      LOGGER.error("bigtype_code must not be null");
      return "";
    }
    if (smalltype_code == null) {
      LOGGER.error("smalltype_code must not be null");
      return "";
    }
    String res = "";
    if (flag == 3) {
      if (appSmallTypeMap.containsKey(bigtype_code + "_" + smalltype_code)) {
        res = appSmallTypeMap.get(bigtype_code + "_" + smalltype_code).toString();
      }
    } else {
      LOGGER.error("this flag is not supported");
    }
    return res;
  }
}
