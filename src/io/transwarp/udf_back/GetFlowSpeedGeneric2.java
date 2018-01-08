package io.transwarp.udf_back;

import io.transwarp.utils.DateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;

/**
 * Created by root on 3/15/17.
 * Calculate the speed of sum_flow.
 */
public class GetFlowSpeedGeneric2 extends AbstractGenericUDAFResolver {

  private static ArrayList<Integer> sumFlow_list = new ArrayList<>();
  private static ArrayList<String> beginTime_list = new ArrayList<>();
  private static ArrayList<String> endTime_list = new ArrayList<>();

  static final Log LOG = LogFactory.getLog(GetFlowSpeedGeneric2.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    // TODO: 1. Type-checking goes here!
    if (parameters.length != 4) {
      throw new UDFArgumentTypeException(parameters.length - 1, "Please specify exactly 4 arguments.");
    }

    for (TypeInfo param : parameters) {
      ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(param);

      if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(0, "Argument must be PRIMITIVE, but " + oi.getCategory().name() + " was passed.");
      }

      PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) oi;

      if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
        throw new UDFArgumentTypeException(0, "Argument must be String, but " + inputOI.getPrimitiveCategory().name() + " was passed.");
      }
    }
    return new GetFlowSpeedEvaluator();
  }

  public static class GetFlowSpeedEvaluator extends GenericUDAFEvaluator {
    // UDAF logic goes here!
    double speed = 0;

    PrimitiveObjectInspector inputOI;
    ObjectInspector outputOI;
    PrimitiveObjectInspector doubleOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {

      assert (parameters.length == 4);
      super.init(m, parameters);

      //map阶段读取sql列，输入为String基础数据格式
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        for (ObjectInspector param : parameters) {
          inputOI = (PrimitiveObjectInspector) param;
        }
      } else {
        //其余阶段，输入为Double基础数据格式
        for (ObjectInspector param : parameters) {
          doubleOI = (PrimitiveObjectInspector) param;
        }
      }

      // 指定各个阶段输出数据格式都为Double类型
      outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
      return outputOI;

    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new LetterSumAgg();
    }

    @Override
    public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
      LetterSumAgg myagg = new LetterSumAgg();
    }

    @Override
    public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
      assert (objects.length == 4);
      String sum_flow = (String) objects[0];
      String begin_time = (String) objects[1];
      String end_time = (String) objects[2];
      String flow_type = (String) objects[3];
      if (sum_flow != null && !sum_flow.equals("") && begin_time != null && !begin_time.equals("") && end_time != null && !end_time.equals("")) {
        try {
          Integer sf = Integer.parseInt(sum_flow);
          sumFlow_list.add(sf);
          beginTime_list.add(begin_time);
          endTime_list.add(end_time);
        } catch (NumberFormatException e) {
//                    e.printStackTrace();
          System.out.println("sum_flow '" + sum_flow + "' is not a int number.");
        }
      }
      LetterSumAgg myagg = (LetterSumAgg) aggregationBuffer;
      myagg.calculate(sumFlow_list, beginTime_list, endTime_list);
      myagg.forSpeed();
    }

    @Override
    public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
      LetterSumAgg myagg = (LetterSumAgg) aggregationBuffer;
      speed += myagg.speedSum / myagg.count;
      return speed;
    }

    @Override
    public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {

    }

    @Override
    public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
      LetterSumAgg myagg = (LetterSumAgg) aggregationBuffer;
      speed += myagg.speedSum / myagg.count;
      return speed;
    }

    static class LetterSumAgg implements AggregationBuffer {

      ArrayList<Integer> sf_tmp = new ArrayList<>();
      ArrayList<String> bt_tmp = new ArrayList<>();
      ArrayList<String> et_tmp = new ArrayList<>();
      double speedSum = 0;
      int count = 0;

      /**
       * @param sumFlows:
       * @param beginTimes:
       * @param endTimes:
       */
      void calculate(ArrayList<Integer> sumFlows, ArrayList<String> beginTimes, ArrayList<String> endTimes) {
        sf_tmp.add(sumFlows.get(0));
        bt_tmp.add(beginTimes.get(0));
        et_tmp.add(endTimes.get(0));
        for (int i = 1; i < sumFlows.size(); i++) {
          Integer sf = sumFlows.get(i);
          String bt = beginTimes.get(i);
          String et = endTimes.get(i);
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
      }

      void forSpeed() {
        count = sf_tmp.size();
        for (int k = 0; k < sf_tmp.size(); k++) {
          Integer sf = sf_tmp.get(k);
          String bt = bt_tmp.get(k);
          String et = et_tmp.get(k);
          double sec = DateFormat.dateMinus(et, bt);
          speedSum += sf / sec;
        }
      }
    }
  }
}
