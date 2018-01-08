package io.transwarp.dao;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class LocalBaseDao {
  private static ConnectionPool cp = null;

  /**
   * 初始化数据库连接池
   */
  private LocalBaseDao() {
  }

  public static ConnectionPool getConnectionPool() {
    if (cp == null) {
      Properties pro = new Properties();
      try {
        pro.load(LocalBaseDao.class.getClassLoader().getResourceAsStream("connection.properties"));
      } catch (IOException e) {
        e.printStackTrace();
      }
      cp = new ConnectionPool(pro.getProperty("local_driver"), pro.getProperty("local_dbUrl"), pro.getProperty("local_dbUser"), pro.getProperty("local_dbPassword"));
      try {
        cp.createPool();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return cp;
  }

  /**
   * 释放资源
   */
  public static void releaseResource(ResultSet rs, PreparedStatement pst, Statement st) {
    try {
      if (rs != null) {
        rs.close();
      }
      if (st != null) {
        st.close();
      }
      if (pst != null) {
        pst.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
