package io.transwarp.test;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by root on 3/18/17.
 */
public class TestSplitPieces {

  private static final ArrayList<String> streamingContentTypeList = new ArrayList<>(Arrays.asList("MP2T", "application/zip", "application/rar", "audio/mp3", "audio/mp4", "audio/midi", "video/3gp", "video/flv", "video/mp4", "video/mpeg"));

  public static void main(String[] args) {
    for (String sct : streamingContentTypeList) {
      System.out.println(sct);
    }
//        rmListElement();
  }

  public static void rmListElement() {
    ArrayList<Integer> sumFlow_list = new ArrayList<>();
    sumFlow_list.add(1);
    sumFlow_list.add(2);
    System.out.println(sumFlow_list.size());
    ArrayList<Integer> indexToRmList = new ArrayList<>();
    indexToRmList.add(0);
    indexToRmList.add(1);
    for (Integer rmIndex : indexToRmList) {
      System.out.println("index to remove: " + rmIndex);
      System.out.println(sumFlow_list.remove(rmIndex.intValue()));
    }
    indexToRmList.clear();
    System.out.println(sumFlow_list.size());
  }

  public static void splitPieces() {
    ArrayList<Integer> sumFlow_list = new ArrayList<>();
    ArrayList<String> beginTime_list = new ArrayList<>();
    ArrayList<String> endTime_list = new ArrayList<>();
    sumFlow_list.add(1);
    beginTime_list.add("2016-08-16 23:53:43.1615535");
    endTime_list.add("2016-08-16 23:53:48.6615535");
    sumFlow_list.add(2);
    beginTime_list.add("2016-08-16 23:53:44.1615535");
    endTime_list.add("2016-08-16 23:53:49.6615535");
    sumFlow_list.add(3);
    beginTime_list.add("2016-08-16 23:53:50.1615535");
    endTime_list.add("2016-08-16 23:53:53.6615535");
    sumFlow_list.add(4);
    beginTime_list.add("2016-08-16 23:53:54.1615535");
    endTime_list.add("2016-08-16 23:53:55.6615535");

    sumFlow_list.add(5);
    beginTime_list.add("2016-08-16 23:53:30.1615535");
    endTime_list.add("2016-08-16 23:53:59.6615535");

    ArrayList<Integer> sf_tmp = new ArrayList<>();
    ArrayList<String> bt_tmp = new ArrayList<>();
    ArrayList<String> et_tmp = new ArrayList<>();
    sf_tmp.add(sumFlow_list.get(0));
    bt_tmp.add(beginTime_list.get(0));
    et_tmp.add(endTime_list.get(0));
    int prCount = sumFlow_list.size();
    int tmpCount = sf_tmp.size();
    for (int i = 1; i < prCount; i++) {
      Integer sf = sumFlow_list.get(i);
      String bt = beginTime_list.get(i);
      String et = endTime_list.get(i);
      for (int j = 0; j < sf_tmp.size(); j++) {
        boolean flag = true;
        Integer sf_j = sf_tmp.get(j);
        String bt_j = bt_tmp.get(j);
        String et_j = et_tmp.get(j);
        if (bt_j.compareTo(et) > 0 || bt.compareTo(et_j) > 0) {
          // add to tmp
//                    sf_tmp.add(sf);
//                    bt_tmp.add(bt);
//                    et_tmp.add(et);
//                    j++;
        } else {
          flag = false;
          String min = bt_j.compareTo(bt) < 0 ? bt_j : bt;
          String max = et_j.compareTo(et) > 0 ? et_j : et;
          sf_tmp.remove(j);
          bt_tmp.remove(j);
          et_tmp.remove(j);
          sf_tmp.add(sf + sf_j);
          bt_tmp.add(min);
          et_tmp.add(max);
        }
      }
    }
    sumFlow_list = new ArrayList<>();
    sumFlow_list.addAll(sf_tmp);
    beginTime_list = new ArrayList<>();
    beginTime_list.addAll(bt_tmp);
    endTime_list = new ArrayList<>();
    endTime_list.addAll(et_tmp);
//        sumFlow_list = sf_tmp;
    sf_tmp.clear();
    bt_tmp.clear();
    et_tmp.clear();
    System.out.println("sum_flow: " + sumFlow_list.size());
    for (Integer sf : sumFlow_list) {
      System.out.println(sf);
    }
    System.out.println("st: " + beginTime_list.size());
    for (String bt : beginTime_list) {
      System.out.println(bt);
    }
    System.out.println("et: " + endTime_list.size());
    for (String et : endTime_list) {
      System.out.println(et);
    }
  }
}
