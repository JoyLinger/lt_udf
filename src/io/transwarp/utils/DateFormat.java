package io.transwarp.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Calendar;

/**
 * Created by root on 3/15/17.
 * 1. Split date in string-regex like "2016-08-16 23:53:43.1615535"
 * 2. Minus endDate and startDate
 * 3. Return time interval in seconds.
 * 4. Double & Float
 * (1)Double: okay
 * 0.009707999999999828
 * 0.004853999999999914
 * (2)Float: wrong
 * 0.009708405
 * 0.0048542025
 */
public class DateFormat {

  private static final Log LOGGER = LogFactory.getLog(DateFormat.class.getName());

  public static double dateMinus(String endDate, String startDate) {
    double res = 0;
    String[] complete_timestamp = endDate.split(" ");
    String[] start_timestamp = startDate.split(" ");
    int days_sec = 0;
    try {
      if (!complete_timestamp[0].equals(start_timestamp[0])) {
        String[] complete_date = complete_timestamp[0].split("-");
        String[] start_date = start_timestamp[0].split("-");

        Calendar cal = Calendar.getInstance();
        cal.set(Integer.parseInt(complete_date[0]), Integer.parseInt(complete_date[1]), Integer.parseInt(complete_date[2]));
        long complete_time = cal.getTimeInMillis();
        cal.set(Integer.parseInt(start_date[0]), Integer.parseInt(start_date[1]), Integer.parseInt(start_date[2]));
        long start_time = cal.getTimeInMillis();
        days_sec = Integer.parseInt((complete_time - start_time) / 1000 + "");
      }
      String[] complete_time = complete_timestamp[1].split(":");
      String[] start_time = start_timestamp[1].split(":");
      int hour = Integer.parseInt(complete_time[0]) - Integer.parseInt(start_time[0]);
      int min = Integer.parseInt(complete_time[1]) - Integer.parseInt(start_time[1]);
      double sec = Double.parseDouble(complete_time[2]) - Double.parseDouble(start_time[2]);
      res = days_sec + hour * 3600 + min * 60 + sec;
    } catch (Exception e) {
      LOGGER.error("Error msg is: " + e.getMessage());
    }
    return res;
  }

  public static void main(String[] args) {
    double duration = dateMinus("2016-08-16 23:53:43.1615535", "2016-08-16 23:53:42.1615535");
    System.out.println("duration: " + duration);
  }
}
