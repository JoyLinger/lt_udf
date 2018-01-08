package io.transwarp.test;

import io.transwarp.dao.JedisPoolManager;
import io.transwarp.udf.hbaseApi.GetPhoneByIMSI;
import io.transwarp.utils.DateFormat;
//import org.mapdb.DB;
//import org.mapdb.DBMaker;


/**
 * Created by zaish on 2016-11-8.
 */
public class Test {
  private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
  public static final JedisPoolManager jdpm = JedisPoolManager.getMgr();
  ;

  public static void main(String[] args) {
//        String jdbcURL = "jdbc:hive2://172.16.2.92:10000/lt_stream";
//        Connection conn=null;
//        PreparedStatement prest = null;
//        ResultSet rs = null;
//        try {
//            Class.forName(driverName);
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//            System.exit(1);
//        }
//        conn = DriverManager.getConnection(jdbcURL);
//        try{
//            String sql = " select PROVINCE_NAME,CITY_NAME,LONGITUDE_BUILD,LATITUDE_BUILD from hb_lac_info where key.lac=? and key.cellid=?";
//            prest = conn.prepareStatement(sql);
//            prest.setString(1,"1819");
//            prest.setString(2,"64601");
//            rs = prest.executeQuery();
//            while(rs.next()){
//                System.out.println(rs.getString(1));
//            }
//        }catch(Exception e){
//            e.printStackTrace();
//        }finally {
//            try {
//                rs.close();
//                prest.close();
//                conn.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//          System.out.println(GetLacInfoTest.evaluate("1819","64601"));
//        System.out.println(GetLacInfoTest.evaluate("1819","64601",2));
//        System.out.println(GetLacInfoTest.evaluate("1819","64601",3));
//        System.out.println(GetLacInfoTest.evaluate("1819","64601",4));
//        System.out.println(GetLacInfoTest.evaluate("1819","64601",5));

//        System.out.println(GetAppInfo.evaluate("1",2));//即时通信
//        System.out.println(GetAppInfo.evaluate("100",1));//彩信业务(发)
//        System.out.println(GetAppInfo.evaluate("1","1",3));//QQ
//          System.out.println(GetAppInfo.evaluate("-1000117067",4));
//        System.out.println(GetAppInfo.evaluate("-1000117067",5));
//appinfo:-1000117067
//appinfo:-1000117067

//        System.out.println(GetTerminalInfo.evaluate("11111111",1));
//        System.out.println(GetTerminalInfo.evaluate("86225002",2));
//        System.out.println(GetTerminalInfo.evaluate("86225002",3));
//        System.out.println(GetTerminalInfo.evaluate("862250021",2));
//        System.out.println(Runtime.getRuntime().maxMemory());
//        Properties pro=new Properties();
//        try {
//            pro.load(LoadData.class.getClassLoader().getResourceAsStream("connection.properties"));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        String url=pro.getProperty("dbUrl");
//        String user=pro.getProperty("dbUser");
//        String passwd=pro.getProperty("dbPassword");
//        System.out.println(passwd);

//        Map map=new HashMap();
//        map.put(1,1);
//        String a=map.get(2).toString();
//        System.out.println(a);
//        DB db = DBMaker.newMemoryDirectDB().transactionDisable().make();
//        ConcurrentMap map = db.getHashMap("map");
//        for(long counter=0;;counter++){
//            map.put(counter,"");
//            if(counter%1000000==0) System.out.println(""+counter);
//        }
//        System.out.println(sun.misc.VM.maxDirectMemory());
//        long start=System.currentTimeMillis();
//        for(int i=0;i<15000;i++){
//            new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    Jedis jedis=jdpm.getResource();
//                    jedis.auth("foobared");
//                    jedis.close();
//                }
//            }).start();
//            if(i%10000==0){
//                System.out.println(i);
//            }
//        }
//        long end=System.currentTimeMillis();
//        System.out.println(end-start);

    /**
     SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
     System.out.println(sdf.format(new Date()));

     System.out.println(testGetPhoneByIMSI(""));
     */

    /**
     String dm = testDateMinus("2017-07-08 23:57:18.3887000","2017-07-08 23:57:18.3789920");
     System.out.println(dm);
     System.out.println(Double.parseDouble(dm) / 2);
     //0.009707999999999828
     //0.004853999999999914
     */

    System.out.println(testUDF());

  }

  private static String testGetPhoneByIMSI(String arg) {
    return GetPhoneByIMSI.evaluate(arg);
  }

  private static String testDateMinus(String endDate, String startDate) {
    return DateFormat.dateMinus(endDate, startDate) + "";
  }

//    private String testGetFlowSpeed(){
//        return GetFlowSpeed.GetSpeedUDAFEvaluator.iterate("1024000","2017-05-03 06:23:39.2175240","2017-05-03 06:23:39.4323810","401","");
//    }

  private static String testUDF() {
    TestUDF1 udf = new TestUDF1();
    return udf.evaluate("29");
  }
}
