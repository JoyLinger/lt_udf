package io.transwarp.test;

import org.apache.hadoop.hive.ql.exec.UDF;

public class HelloUdf extends UDF {
  public static String evaluate(String user) {
    return "Hello" + user;
  }
}
