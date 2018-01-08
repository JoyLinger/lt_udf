package io.transwarp.test;

/**
 * Created by root on 3/15/17.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.expr.ExprFactory;
import org.apache.hadoop.hbase.expr.ExprInterface;
import org.apache.hadoop.hbase.expr.result.BytesResult;
import org.apache.hadoop.hbase.expr.result.CollectionResult;
import org.apache.hadoop.hbase.filter.ExprFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class HyperbaseStructTest {

  private static Configuration conf = null;
  private static Properties prop = new Properties();
  private static String zk;
  private static String namespace;
  private static String tableName;

  static {
    try {
      prop.load(HyperbaseStructTest.class.getClassLoader().getResourceAsStream("hbaseApi.properties"));
    } catch (Exception e) {
      e.printStackTrace();
    }
    zk = prop.getProperty("zookeeper");
    namespace = prop.getProperty("namespace");
    tableName = prop.getProperty("imsi_phone_table");
    conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", zk);
  }

  public static void main(String[] args) {
    try {
      scan();
//            HTable table = new HTable(conf,(namespace + ":" + tableName).getBytes());
//            Get get = new Get("862250020000668xx".getBytes());
//            // keyvalues=NONE
//            Result rs = table.get(get);
//            System.out.println(rs.isEmpty() ? "" : new String(rs.getValue("f".getBytes(),"q1".getBytes()),"UTF-8"));

//            String rowkey = getAllRecord(table);
//            System.out.println("rowkey:" + rowkey);
//            getOneRecord(table,rowkey);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * 查找一行记录
   */
  public static void getOneRecord(HTable htable, String rowKey) throws IOException {
    byte[] b = rowKey.getBytes();
    Get get = new Get(b);
    Result rs = htable.get(get);
    for (KeyValue kv : rs.raw()) {
      System.out.print(new String(kv.getRow()) + " ");
      System.out.print(new String(kv.getFamily()) + ":");
      System.out.print(new String(kv.getQualifier()) + " ");
      System.out.print(kv.getTimestamp() + " ");
      System.out.println(new String(kv.getValue()));
    }
    htable.close();
  }

  /**
   * 返回rowkey
   */
  public static String getAllRecord(HTable htable) {
    try {
      Scan s = new Scan();
      ResultScanner ss = htable.getScanner(s);
      for (Result r : ss) {
        for (KeyValue kv : r.raw()) {
          return new String(kv.getRow());
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }


  public static void scan() {
//        IndexHTable hTable = null;
    HTable hTable = null;
    try {
      hTable = new HTable(conf, "others:hb_lac_info");
      Scan scan = new Scan();
      scan.setFilter(getInFilter("f".getBytes(), "q1".getBytes(), "1819".getBytes()));
      scan.setFilter(getInFilter("f".getBytes(), "q2".getBytes(), "64601".getBytes()));

      scan.setIndexColumn(Bytes.toBytes("others:hb_lac_info_lac_cell_index"));

      ResultScanner rs = hTable.getScanner(scan);
      Result result = null;
      String key = "";
      while (null != (result = rs.next())) {
        key = new String(result.getRow(), "UTF-8");
        System.out.println("================" + key);
      }
      getOneRecord(hTable, key);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static Filter getInFilter(byte[] family, byte[] qualifier, byte[]... c1) {
    ExprInterface kobhVarExpr = ExprFactory.createVariableExpr(family, qualifier);
    Collection<org.apache.hadoop.hbase.expr.result.Result> colls = new ArrayList<>();
    for (byte[] c : c1) {
      colls.add(BytesResult.getInstance(c));
    }
    org.apache.hadoop.hbase.expr.result.Result result = CollectionResult.getInstance(colls);
    ExprInterface kkbhValueExpr = ExprFactory.createConstExpr(result);
    ExprInterface funcExpr = ExprFactory.createInExpr(kobhVarExpr, kkbhValueExpr);
    return new ExprFilter(funcExpr);
  }
}

