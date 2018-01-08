package io.transwarp.udf.util;

import io.transwarp.utils.LoadData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Map;

/**
 * Created by zaish on 2016-11-3.
 */
public class GetTerminalInfo extends UDF {

  private static final Log LOGGER = LogFactory.getLog(GetTerminalInfo.class.getName());
  private static Map manu_nameMap;
  private static Map model_nameMap;

  static {
    LoadData ld = LoadData.getInstance();
    manu_nameMap = ld.getManu_nameMap();
    model_nameMap = ld.getModel_nameMap();
  }

  /*
   * 输入：tar码与标志位
   * 输出：flag=1 返回manu_name;  flag=2 返回model_name
   */
  public static String evaluate(String tac, int flag) {
    if (tac == null) {
      LOGGER.error("lac must not be null");
      return "";
    }
    if (tac.length() != 8) {
      LOGGER.error("length of tac must be 8");
    }
    String res = "";
    if (flag == 1) {
      if (manu_nameMap.containsKey(tac)) {
        res = manu_nameMap.get(tac).toString();
      }
    } else if (flag == 2) {
      if (model_nameMap.containsKey(tac)) {
        res = model_nameMap.get(tac).toString();
      }
    } else {
      LOGGER.error("this flag is not supported");
    }
    return res == null ? "" : res;
  }
}
