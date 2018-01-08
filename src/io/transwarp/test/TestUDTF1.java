package io.transwarp.test;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


/**
 *  * GenericUDTFCount2 outputs the number of rows seen, twice. It's output twice
 *  * to test outputting of rows on close with lateral view.
 *  *
 *  
 */
public class TestUDTF1 extends GenericUDTF {

  Integer count = Integer.valueOf(0);
  Object forwardObj[] = new Object[1];

  @Override
  public void close() throws HiveException {
    forwardObj[0] = count;
    forward(forwardObj);
    forward(forwardObj);
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
    Properties prop = new Properties();
    try {
      prop.load(TestUDTF1.class.getClassLoader().getResourceAsStream("connection.properties"));
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
    String sql = "select '" + TestUDTF1.class.getName() + "' from system.dual";
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
    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    fieldNames.add("col1");
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    count = Integer.valueOf(count.intValue() + 1);
  }
}