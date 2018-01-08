package io.transwarp.utils;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zaish on 2016-11-14.
 */
public class LoadData {

  private static final Log LOGGER = LogFactory.getLog(LoadData.class.getName());

  private String driverName = "org.apache.hive.jdbc.HiveDriver";
  private static LoadData instance;
  private static Map<String, String> manu_nameMap;
  private static Map<String, String> model_nameMap;
  private static Map<String, String> appBigTypeMap;
  private static Map<String, String> appSmallTypeMap;
  private static Map<String, String> dataTypeMap;

  private LoadData() {
    this.manu_nameMap = new HashMap();
    this.model_nameMap = new HashMap();
    this.appBigTypeMap = new HashMap();
    this.appSmallTypeMap = new HashMap();
    this.dataTypeMap = new HashMap();
    String sep = "_";
    Properties pro = new Properties();
    try {
      pro.load(LoadData.class.getClassLoader().getResourceAsStream("connection.properties"));
    } catch (IOException e) {
      LOGGER.error("Error msg is: " + e.getMessage());
//            e.printStackTrace();
    }
    String url = pro.getProperty("dbUrl");
    String user = pro.getProperty("dbUser");
    String passwd = pro.getProperty("dbPassword");
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      LOGGER.error("Error msg is: " + e.getMessage());
//            e.printStackTrace();
//            System.exit(1);
    }
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    String sql = "";
    try {
      conn = DriverManager.getConnection(url, user, passwd);
      stmt = conn.createStatement();
      //load终端信息
      rs = stmt.executeQuery("select tac,manu_name,model_name from orc_imei_terminal;");
      while (rs.next()) {
        manu_nameMap.put(rs.getString(1), rs.getString(2));
        model_nameMap.put(rs.getString(1), rs.getString(3));
      }
      //load 应用大小类
      sql = "select b.bigtype_code,s.smalltype_code,b.bigtype,s.smalltype from " +
              "orc_app_bigtype b join orc_app_smalltype s " +
              "on b.bigtype_code=s.bigtype_code;";
      rs = stmt.executeQuery(sql);
      while (rs.next()) {
        appSmallTypeMap.put(rs.getString(1) + sep + rs.getString(2), rs.getString(3) + sep + rs.getString(4));
        appBigTypeMap.put(rs.getString(1), rs.getString(3));
      }
      //load 流量类型
      rs = stmt.executeQuery("select business_code,desc from orc_data_type");
      while (rs.next()) {
        dataTypeMap.put(rs.getString(1), rs.getString(2));
      }
    } catch (SQLException e) {
      LOGGER.error("Error msg is: " + e.getMessage());
//            e.printStackTrace();
    } finally {
      try {
        rs.close();
        stmt.close();
        conn.close();
      } catch (SQLException e) {
        LOGGER.error("Error msg is: " + e.getMessage());
//                e.printStackTrace();
      }
    }
  }

  public Map getManu_nameMap() {
    return manu_nameMap;
  }

  public Map getModel_nameMap() {
    return model_nameMap;
  }

  public Map getDataTypeMap() {
    return dataTypeMap;
  }

  public static Map<String, String> getAppSmallTypeMap() {
    return appSmallTypeMap;
  }

  public static Map<String, String> getAppBigTypeMap() {
    return appBigTypeMap;
  }

  public static LoadData getInstance() {
    if (instance == null) {
      instance = new LoadData();
    }
    return instance;
  }

  public static void main(String[] args) {
    LoadData ld = new LoadData();
    System.out.println(ld.getAppSmallTypeMap().size());
    System.out.println(ld.getDataTypeMap().size());
    System.out.println(ld.getManu_nameMap().size());
    System.out.println(ld.getModel_nameMap().size());
    for (Object object : ld.getAppSmallTypeMap().entrySet()) {
      Map.Entry<String, String> entry = (Map.Entry<String, String>) object;
      System.out.println("key = " + entry.getKey() + " and value = " + entry.getValue());
    }
  }
}
