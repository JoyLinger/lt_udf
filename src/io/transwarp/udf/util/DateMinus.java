package io.transwarp.udf.util;

import io.transwarp.utils.DateFormat;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Calendar;

/**
 * Created by root on 1/11/17.
 * Minus endDate and startDate.
 */
public class DateMinus extends UDF {
  public static String evaluate(String endDate, String startDate) {
    if (startDate == null || "".equals(startDate) || endDate == null || "".equals(endDate)) {
      return null;
    } else {
      return DateFormat.dateMinus(endDate, startDate) + "";
    }
  }
}
