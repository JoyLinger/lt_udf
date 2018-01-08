package io.transwarp.test;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/**
 * Created by root on 10/12/17.
 */
class TestUDF1 extends UDF {
  public static String evaluate(String x) {
//        System.out.println(TestUDF.class.getName());
    Properties prop = new Properties();
    try {
      prop.load(TestUDF1.class.getClassLoader().getResourceAsStream("connection.properties"));
    } catch (IOException e) {
      e.printStackTrace();
    }
    String driver = prop.getProperty("driver");
    String url = prop.getProperty("dbUrl");
    String user = prop.getProperty("dbUser");
    String passwd = prop.getProperty("dbPassword");
    Connection conn;
    Statement stmt;
    ResultSet rs;
    String sql = "select '" + TestUDF1.class.getName() + "' from system.dual";
    String result = "";
    try {
      Class.forName(driver);
      conn = DriverManager.getConnection(url, user, passwd);
      stmt = conn.createStatement();
      //load终端信息
      rs = stmt.executeQuery(sql);
      result = rs.getString(1);
    } catch (ClassNotFoundException | SQLException e) {
      e.printStackTrace();
    }
    return result + ": " + Double.valueOf(x) * 2;
  }
}
