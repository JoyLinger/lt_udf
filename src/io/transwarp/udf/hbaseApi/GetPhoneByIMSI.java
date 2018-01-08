package io.transwarp.udf.hbaseApi;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;

/**
 * Created by zaish on 2016-11-3.
 * 输入：imsi
 * 输出：phone,若无匹配值返回""
 */
public class GetPhoneByIMSI extends UDF {

  private static final Log LOGGER = LogFactory.getLog(GetPhoneByIMSI.class.getName());
  private static final String NAMESPACE = "others";
  private static final String TABLE_NAME = "hb_imsi_phone";
  private static final String FAMILY = "f";
  private static final String QUALIFIER = "q1";

  private static Configuration CONF = null;


  static {
    Configuration hbConf = new Configuration();
    hbConf.addResource(GetLacInfo.class.getClassLoader().getResourceAsStream("core-site.xml"));
    hbConf.addResource(GetLacInfo.class.getClassLoader().getResourceAsStream("hdfs-site.xml"));
    hbConf.addResource(GetLacInfo.class.getClassLoader().getResourceAsStream("hbase-site.xml"));
    CONF = HBaseConfiguration.create(hbConf);
  }


  public static String evaluate(String imsi) {
    if (imsi == null || "".equals(imsi)) {
      return "";
    }
    byte[] TABLE_NAMEBytes = (NAMESPACE + ":" + TABLE_NAME).getBytes();
    byte[] imsiBytes = (imsi).getBytes();
    try {
      HBaseAdmin hAdmin = new HBaseAdmin(CONF);
      if (hAdmin.isTableDisabled(TABLE_NAMEBytes)) {
        hAdmin.enableTable(TABLE_NAMEBytes);
      }
      Get get = new Get(imsiBytes);
      HTable hTable = new HTable(CONF, TABLE_NAMEBytes);
      Result rs = hTable.get(get);
      return rs.isEmpty() ? "" : new String(rs.getValue(FAMILY.getBytes(), QUALIFIER.getBytes()), "UTF-8");
    } catch (IOException e) {
      LOGGER.error("Error msg is: " + e.getMessage());
    }
    return "";
  }
}
