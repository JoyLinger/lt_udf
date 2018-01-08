package io.transwarp.udf.util;

import io.transwarp.utils.DateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by root on 3/15/17.
 * Calculate the speed of sum_flow.
 */
public class GetFlowSpeed1 extends UDAF {

  private static final Log LOGGER = LogFactory.getLog(GetFlowSpeed1.class.getName());
  private static final ArrayList<String> STREAMING_CONTENT_TYPE_LIST = new ArrayList<>(Arrays.asList("MP2T", "application/zip", "application/rar", "audio/mp3", "audio/mp4", "audio/midi", "video/3gp", "video/flv", "video/mp4", "video/mpeg"));
  private static final ArrayList<String> STREAMING_FLOW_TYPE_LIST = new ArrayList<>(Arrays.asList("200", "201", "204", "205"));

  public static class PartialResult {
    private ArrayList<Integer> sumFlow_list;
    private ArrayList<String> beginTime_list;
    private ArrayList<String> endTime_list;
    private int count;
//            String sumFlow;
//            String beginTime;
//            String endTime;
//            String flowType;
  }

  public static class GetSpeedUDAFEvaluator implements UDAFEvaluator {

    PartialResult pr;

    public GetSpeedUDAFEvaluator() {
      super();
      pr = new PartialResult();
      init();
    }


    private void printLog() {
      LOGGER.info("pr.sum_flow: " + pr.sumFlow_list.size());
      for (Integer sf : pr.sumFlow_list) {
        LOGGER.info(sf);
      }
      LOGGER.info("pr.st: " + pr.beginTime_list.size());
      for (String bt : pr.beginTime_list) {
        LOGGER.info(bt);
      }
      LOGGER.info("pr.et: " + pr.endTime_list.size());
      for (String et : pr.endTime_list) {
        LOGGER.info(et);
      }
    }


    private void setCount(PartialResult pr) {
      if (pr.sumFlow_list.size() == pr.beginTime_list.size() && pr.beginTime_list.size() == pr.endTime_list.size()) {
        pr.count = pr.sumFlow_list.size();
      } else {
        LOGGER.error("PartialResult is error");
      }
    }

    private void splitPieces2(Integer sum_flow_int, String begin_time, String end_time) {
      if (pr.count == 0) {
        pr.sumFlow_list.add(sum_flow_int);
        pr.beginTime_list.add(begin_time);
        pr.endTime_list.add(end_time);
        setCount(pr);
      } else {
        Integer sum_sf = sum_flow_int;
        String min_bt = begin_time;
        String max_et = end_time;
        ArrayList<Integer> indexToRmList = new ArrayList<>();
        for (int i = 0; i < pr.count; i++) {
          Integer sf = pr.sumFlow_list.get(i);
          String bt = pr.beginTime_list.get(i);
          String et = pr.endTime_list.get(i);
          if (!(begin_time.compareTo(et) > 0 || bt.compareTo(end_time) > 0)) {
            sum_sf += sf;
            min_bt = min_bt.compareTo(bt) < 0 ? min_bt : bt;
            max_et = max_et.compareTo(et) > 0 ? max_et : et;
            indexToRmList.add(i);
          }
        }
        for (int j = 0; j < indexToRmList.size(); j++) {
          // Once remove, then size minus 1. So rmIndex minus 1 too!
          Integer rmIndex = indexToRmList.get(j) - j;
//                    LOGGER.info("index to remove: " + rmIndex);
          // Remove element using index with type int, not Integer!
          pr.sumFlow_list.remove(rmIndex.intValue());
          pr.beginTime_list.remove(rmIndex.intValue());
          pr.endTime_list.remove(rmIndex.intValue());
        }
        indexToRmList.clear();
        pr.sumFlow_list.add(sum_sf);
        pr.beginTime_list.add(min_bt);
        pr.endTime_list.add(max_et);
        setCount(pr);
      }
    }

    /**
     * Reset the state of the aggregation.
     */
    @Override
    public void init() {
      pr.sumFlow_list = new ArrayList<>();
      pr.beginTime_list = new ArrayList<>();
      pr.endTime_list = new ArrayList<>();
      pr.count = 0;
    }

    /**
     * Iterate through one row of original data.
     * <p>
     * The number and type of arguments need to the same as we call this
     * UDAF from Hive command line.
     * <p>
     * This function should always return true.
     */
    public boolean iterate(String sum_flow, String begin_time, String end_time, String flow_type, String contentType) {
//            LOGGER.info("*****iterate*****");
      Integer sum_flow_int;
      try {
        sum_flow_int = Integer.parseInt(sum_flow);
      } catch (NumberFormatException e) {
        LOGGER.info("sum_flow '" + sum_flow + "' is not a int number.");
        return true;
      }
      if (pr == null) {
        pr = new PartialResult();
        pr.sumFlow_list = new ArrayList<>();
        pr.beginTime_list = new ArrayList<>();
        pr.endTime_list = new ArrayList<>();
      }
      if (flow_type != null && !flow_type.equals("") && flow_type.length() == 3) {
        if (flow_type.startsWith("4") || (STREAMING_FLOW_TYPE_LIST.contains(flow_type) && STREAMING_CONTENT_TYPE_LIST.contains(contentType))) {
          // STREAMING
          if (sum_flow_int < 480 * 1024) {
            return true;
          }
        } else if (flow_type.startsWith("2")) {
          // HTTP
          if (sum_flow_int < 200 * 1024) {
            return true;
          }
        } else {
          // OTHERS
          if (sum_flow_int < 200 * 1024 && sum_flow_int / DateFormat.dateMinus(end_time, begin_time) < 50 * 1024) {
            return true;
          }
        }
      }
      if (!sum_flow.equals("") && begin_time != null && !begin_time.equals("") && end_time != null && !end_time.equals("")) {
        splitPieces2(sum_flow_int, begin_time, end_time);
//                printLog();
      }
      return true;
    }

    /**
     * Terminate a partial aggregation and return the state. If the state is
     * a primitive, just return primitive Java classes like Integer or
     * String.
     */
    public PartialResult terminatePartial() {
//            LOGGER.info("****terminatePartial******");
//            printLog();
      return pr;
    }

    /**
     * Merge with a partial aggregation.
     * <p>
     * This function should always have a single argument which has the same
     * type as the return value of terminatePartial().
     * <p>
     * 合并
     */
    public boolean merge(PartialResult pr) {
//            LOGGER.info("****merge******");
      if (this.pr == null) {
        this.pr = new PartialResult();
        this.pr.sumFlow_list = new ArrayList<>();
        this.pr.beginTime_list = new ArrayList<>();
        this.pr.endTime_list = new ArrayList<>();
      }
      if (pr != null) {
        this.pr.sumFlow_list.addAll(pr.sumFlow_list);
        this.pr.beginTime_list.addAll(pr.beginTime_list);
        this.pr.endTime_list.addAll(pr.endTime_list);
//                printLog();
//                this.pr = splitPieces(this.pr);
      }
//            for(int i = 0 ; i < pr.sumFlow_list.size() ; i++){
//                iterate(pr.sumFlow_list.get(i).toString(), (pr.beginTime_list.get(i)),pr.endTime_list.get(i),"false");
//            }
      return true;
    }

    /**
     * Terminates the aggregation and return the final result.
     * <p>
     * 计算并返回
     */
    public double terminate() {
      LOGGER.info("****terminate******");
      printLog();
      return calculate(pr);
    }

    /**
     * @return pr
     */
    private static PartialResult splitPieces(PartialResult pr) {
//            LOGGER.info("****splitPieces******");
      if (pr.sumFlow_list.size() == 0 || pr.beginTime_list.size() == 0 || pr.endTime_list.size() == 0) {
        return pr;
      }
      ArrayList<Integer> sf_tmp = new ArrayList<>();
      ArrayList<String> bt_tmp = new ArrayList<>();
      ArrayList<String> et_tmp = new ArrayList<>();
      sf_tmp.add(pr.sumFlow_list.get(0));
      bt_tmp.add(pr.beginTime_list.get(0));
      et_tmp.add(pr.endTime_list.get(0));
      for (int i = 1; i < pr.sumFlow_list.size(); i++) {
        Integer sf = pr.sumFlow_list.get(i);
        String bt = pr.beginTime_list.get(i);
        String et = pr.endTime_list.get(i);
        for (int j = 0; j < sf_tmp.size(); j++) {
          Integer sf_j = sf_tmp.get(j);
          String bt_j = bt_tmp.get(j);
          String et_j = et_tmp.get(j);
          if (bt_j.compareTo(et) > 0 || bt.compareTo(et_j) > 0) {
            // add to tmp
            sf_tmp.add(sf);
            bt_tmp.add(bt);
            et_tmp.add(et);
          } else {
            String min = bt_j.compareTo(bt) < 0 ? bt_j : bt;
            String max = et_j.compareTo(et) > 0 ? et_j : et;
            // add to tmp
            sf_tmp.add(sf + sf_j);
            bt_tmp.add(min);
            et_tmp.add(max);
          }
        }
      }
      pr.sumFlow_list = sf_tmp;
      pr.beginTime_list = bt_tmp;
      pr.endTime_list = et_tmp;
      sf_tmp.clear();
      bt_tmp.clear();
      et_tmp.clear();
      /**
       LOGGER.info("pr.sum_flow: " + pr.sumFlow_list.size());
       for(Integer sf : pr.sumFlow_list){
       LOGGER.info(sf);
       }
       LOGGER.info("pr.st: " + pr.beginTime_list.size());
       for(String bt : pr.beginTime_list){
       LOGGER.info(bt);
       }
       LOGGER.info("pr.et: " + pr.endTime_list.size());
       for(String et : pr.endTime_list){
       LOGGER.info(et);
       }
       */
      return pr;
    }

    private static double calculate(PartialResult pr) {
//            LOGGER.info("****calculate******");
      double speedSum = 0;
      int count = pr.sumFlow_list.size();
      for (int k = 0; k < count; k++) {
        Integer sf = pr.sumFlow_list.get(k);
        String bt = pr.beginTime_list.get(k);
        String et = pr.endTime_list.get(k);
        double sec = DateFormat.dateMinus(et, bt);
        speedSum += sf / sec;
//                LOGGER.info(speedSum + "=" + sf + "/" + sec);
      }
      if (count == 0) {
        return 0;
      }
//            LOGGER.info("speed=" + speedSum + "/" + count);
      return speedSum / count;

    }
  }
}
