package io.transwarp.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Created by root on 3/14/17.
 */
public class GetLacInfoTest {

  private static Configuration conf = null;
  private static Properties prop = new Properties();
  private static String zk;
  private static String namespace;
  private static String tableName;

  static {
    try {
      FileInputStream fis = new FileInputStream("src/hbaseApi.properties");
      prop.load(fis);
    } catch (Exception e) {
      e.printStackTrace();
    }
    zk = prop.getProperty("zookeeper", "hadoop214,hadoop218,hadoop222");
    namespace = prop.getProperty("namespace", "others");
    tableName = prop.getProperty("lac_table", "hb_lac_info");
    conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", zk);
  }

  /*
   * 输入：code与标志位
   * 输出：返回省份_城市_经度_纬度
   */
  public static String evaluate(String lac, String cellid, String netType) {
    String rowKey = lac + cellid + netType;
    byte[] b = {0, 49, 56, 49, 57, 0, 0, 0, 0, 54, 52, 54, 48, 0, 49, 0, -128, 1, 0, 0, 0, 0, 0, 7, 0, 0, 0, 5, 0, 0, 0, 6, 49, 0, 0, 0, 2};
    try {
      HTable table = new HTable(conf, (namespace + ":" + tableName).getBytes());
      Get get = new Get(b);
      Result rs = table.get(get);
      return rs.getValue(Bytes.toBytes("f"), Bytes.toBytes("q6")).toString() + "_" +
              rs.getValue(Bytes.toBytes("f"), Bytes.toBytes("q7")).toString() + "_" +
              rs.getValue(Bytes.toBytes("f"), Bytes.toBytes("q3")).toString() + "_" +
              rs.getValue(Bytes.toBytes("f"), Bytes.toBytes("q4")).toString();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }


  /**
   * 十进制 --> 十六进制
   *
   * @param content
   * @return
   */
  public static String to16(String content) {
    String hexStr = "";
    for (int i = 0; i < content.length(); i++) {
      int ch = content.charAt(i);
      String str = Integer.toHexString(ch);
      hexStr += str;
    }
    return hexStr;
  }


  public static void main(String args[]) {
//        String sixteen = "\\x0061873\\x00\\x00\\x00622\\x00\\x002\\x00\\x80\\x01\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x06\\x00\\x00\\x00\\x04\\x00\\x00\\x00\\x02";
//        String ten = "618736222";
//        System.out.println(to16(ten));
//        System.exit(0);
    System.out.println(evaluate("61873", "622", "2"));
  }

}
