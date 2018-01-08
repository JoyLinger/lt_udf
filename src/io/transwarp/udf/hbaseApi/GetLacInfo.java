package io.transwarp.udf.hbaseApi;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.expr.ExprFactory;
import org.apache.hadoop.hbase.expr.ExprInterface;
import org.apache.hadoop.hbase.expr.result.BytesResult;
import org.apache.hadoop.hbase.expr.result.CollectionResult;
import org.apache.hadoop.hbase.filter.ExprFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by root on 3/14/17.
 * 输入：code--lac & cell
 * 输出：返回省份_城市_经度_纬度 ==> q8_q9_q6_q7
 */
public class GetLacInfo extends UDF {

  private static final Log LOGGER = LogFactory.getLog(GetLacInfo.class);
  private static final String NAMESPACE = "others";
  private static final String TABLE_NAME = "hb_lac_info";
  private static final String INDEX_NAME = "lac_cell_index";
  private static final String FAMILY = "f";
  private static final String[] RS_QUALIFIERS = {"q8", "q9", "q6", "q7"};
  private static final String LAC_QUALIFIER = "q1";
  private static final String CELL_QUALIFIER = "q2";
  private static Configuration CONF = null;

  static {
    Configuration hbConf = new Configuration();
    hbConf.addResource(GetLacInfo.class.getClassLoader().getResourceAsStream("core-site.xml"));
    hbConf.addResource(GetLacInfo.class.getClassLoader().getResourceAsStream("hdfs-site.xml"));
    hbConf.addResource(GetLacInfo.class.getClassLoader().getResourceAsStream("hbase-site.xml"));
    CONF = HBaseConfiguration.create(hbConf);
  }

  /*
   * 输入：code--lac & cell
   * 输出：返回省份_城市_经度_纬度
   */
  public static String evaluate(String lac, String cell) {
    long beginTime = System.currentTimeMillis();
    LOGGER.info("beginTime: " + beginTime);
    byte[] tableNameBytes = (NAMESPACE + ":" + TABLE_NAME).getBytes();
    try {
      // 23009
//            IndexHTable hTable = new IndexHTable(CONF, tableNameBytes);
      // 22973
      HTable hTable = new HTable(CONF, tableNameBytes);
      Scan scan = new Scan();
      scan.setFilter(getInFilter(FAMILY.getBytes(), LAC_QUALIFIER.getBytes(), lac.getBytes()));
      scan.setFilter(getInFilter(FAMILY.getBytes(), CELL_QUALIFIER.getBytes(), cell.getBytes()));
      scan.setIndexColumn(Bytes.toBytes(NAMESPACE + ":" + TABLE_NAME + "_" + INDEX_NAME));
      ResultScanner scanner = hTable.getScanner(scan);
      Result result;
      String key;
      // Only one result, so use 'if'
      if ((result = scanner.next()) != null) {
        key = new String(result.getRow(), "UTF-8");
      } else {
        return " _ _ _ ";
      }
      Get get = new Get(key.getBytes());
      Result rs = hTable.get(get);
      String rt = new String(rs.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(RS_QUALIFIERS[0])), "UTF-8") + "_" +
              new String(rs.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(RS_QUALIFIERS[1])), "UTF-8") + "_" +
              new String(rs.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(RS_QUALIFIERS[2])), "UTF-8") + "_" +
              new String(rs.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(RS_QUALIFIERS[3])), "UTF-8");
      long endTime = System.currentTimeMillis();
      LOGGER.info("endTime: " + endTime);
      LOGGER.info("Used time: " + (endTime - beginTime));
      return rt;
    } catch (Exception e) {
      LOGGER.error("Error msg is: " + e.getMessage());
    }
    return " _ _ _ ";
  }

  /**
   * Get filter.
   *
   * @param family    family
   * @param qualifier column
   * @param c1        value
   * @return ExprFilter
   */
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
