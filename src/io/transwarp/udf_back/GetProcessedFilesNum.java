package io.transwarp.udf_back;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by zaish on 2016-11-24.
 */
public class GetProcessedFilesNum extends UDF {
  public static String evaluate(String path) throws Exception {
    String[] cmds = {"/bin/sh", "-c", "ls " + path + "|grep .complete|wc -l"};
    Process process = Runtime.getRuntime().exec(cmds);
    process.waitFor();
    InputStream in = process.getInputStream();
    BufferedReader read = new BufferedReader(new InputStreamReader(in));
    String result = "";
    String line = null;
    if ((line = read.readLine()) != null) {
      result = line;
    }
    return result;
  }

  public static void main(String[] args) throws Exception {
    System.out.println(evaluate(args[0]));
  }
}
