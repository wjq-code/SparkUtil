package com.wjq.addrparse;

import com.alibaba.fastjson.JSONObject;
import com.wjq.collection.GlobalConstant;
import com.wjq.jdbc.MySqlUtil2;
import com.wjq.job.SqlConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.nutz.dao.Cnd;
import org.nutz.dao.entity.Record;
import org.nutz.dao.impl.NutDao;
import org.nutz.dao.impl.SimpleDataSource;
import org.nutz.lang.Strings;
import scala.Function1;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.mutable.ListBuffer;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.util.*;

/**
 * @author: 根据经纬度获取相对应的地址
 * @data: 2018/8/31 17:58
 * @version:
 */
public class MapUtil {
    public static double pi = 3.1415926535897932384626;
    public static double a = 6378245.0;
    public static double ee = 0.00669342162296594323;
//    public static double a = 6378140.0;//1975年国际椭球体长半轴
//  w  public static double ee = 0.0033528131778969143;//1975年国际椭球体扁率
    /**
     * 百度地图请求秘钥
     */
    private static final String KEY = "cNVb3FE7fqasnYjeHCZl78sa1DBhI7iS";
    private static final String KEY2 = "W1yVHGUHP8sxc0I8CTUSOSVqpxiIP4nQ";
    private static final String KEY6 = "Y3plT3f3UvoSj8Q9un7L8keMA5hiMv8h";
    private static final String KEY7 = "N0tbhC1vLqYpz449GtPv3Yd7Hyi5ovej";
    private static final String KEY8 = "6IXw0AtHZYumEy0iHewrcT4kAopvawdw";
    private static final String COORDTYPE = "wgs84ll";
    static int index = 0;
    private static final String[] arr = {KEY, KEY7, KEY8, KEY2, KEY6};
//    private static final String[] adrr = {KEY2};
    /**
     * 返回值类型
     */
    private static final String OUTPUT = "json";
    /**957
     * 根据地名获取经纬度
     */
    private static final String GET_LNG_LAT_URL = "http://api.map.baidu.com/geocoding/v3/";
    /**
     * 根据经纬度获取地名
     */
    private static final String GET_ADDRESS_URL = "http://api.map.baidu.com/reverse_geocoding/v3/";

    public static void main(String[] args) throws ClassNotFoundException {
        // 地址转经纬度
//        city2Lon();
        // 经纬度转地址
        Lon2city();
    }


    /**
     * 根据经纬度获得省市区信息
     */
    public static Map<String, String> getCityByLonLat(String location) {

        // http://api.map.baidu.com/geocoder/v2/?location=24.630671%2C118.006451&output=json&ak=cNVb3FE7fqasnYjeHCZl78sa1DBhI7iS
        // http://api.map.baidu.com/reverse_geocoding/v3/?ak=你的AK&output=json&coordtype=wgs84ll&location=31.225696,121.49884
        Map<String, String> params = new HashMap<String, String>();
        params.put("ak", arr[index]);
        try {
            //拼装url
            String url = joinUrl(params, OUTPUT, location, GET_ADDRESS_URL);
            System.out.println(url);

            JSONObject result = null;

            try {
                result = test(result, params, location, url);
            } catch (Exception e) {
                result = test(result, params, location, url);
            }
            Map<String, String> area = new HashMap<String, String>();
            try {
                area.put("gjdm", result.getString("country_code_iso"));  // 国籍代码
            } catch (Exception e) {
                area.put("gjdm", "");  // 国籍代码
            }
            try {
                area.put("domestic", result.getString("country_code"));  // 是否境内
            } catch (Exception e) {
                area.put("domestic", "");  // 是否境内
            }
            try {

                area.put("province", result.getString("province")); // 省
            } catch (Exception e) {
                area.put("province", ""); // 省
            }
            try {
                area.put("city", result.getString("city"));  // 市
            } catch (Exception e) {
                area.put("city", "");  // 市
            }
            try {
                area.put("district", result.getString("district"));  // 县
            } catch (Exception e) {
                area.put("district", "");  // 县
            }
//            JSONObject result1 = JSONObject.parseObject(String.valueOf(JSONObject.parseObject(HttpClientUtils.doGet(url))));
            try {
                JSONObject rs = JSONObject.parseObject(String.valueOf(JSONObject.parseObject(JSONObject.parseObject(HttpClientUtils.doGet(url)).
                        getString("result"))));
                area.put("sematic_description", rs.getString("sematic_description"));  // 镇
            } catch (Exception e) {
                area.put("sematic_description", "");
            }

            return area;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 根据省市区信息获得经纬度
     */
    public static Map<String, Double> getLonLatByCity(String location) {
        Map<String, String> params = new HashMap<String, String>();
        try {
            //拼装url
            String url = joinUrl2(arr[index], OUTPUT, location, GET_LNG_LAT_URL);
            System.out.println(url);
            JSONObject result = null;
            try {
                result = test1(result, params, location, url);
            } catch (Exception e) {
                result = test1(result, params, location, url);
            }
            Map<String, Double> area = new HashMap<String, Double>();
            try {
                area.put("lng", result.getDouble("lng"));
            } catch (Exception e) {
                area.put("lng", 0.0);
            }
            try {
                area.put("lat", result.getDouble("lat"));
            } catch (Exception e) {
                area.put("lat", 0.0);
            }
            return area;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 拼接url字符串（经纬度转地址）
     */
    private static String joinUrl(Map<String, String> params, String output, String location, String url) throws IOException {
        StringBuilder baseUrl = new StringBuilder();
        baseUrl.append(url);
        int index = 0;
        Set<Map.Entry<String, String>> entrys = params.entrySet();
        for (Map.Entry<String, String> param : entrys) {
            // 判断是否是第一个参数
            if (index == 0) {
                baseUrl.append("?");
            } else {
                baseUrl.append("&");
            }
            baseUrl.append(param.getKey()).append("=").append(URLEncoder.encode(param.getValue(), "utf-8"));
            index++;
        }
        baseUrl.append("&output=").append(output).append("&coordtype=").append(COORDTYPE).append("&location=").append(location);
        return baseUrl.toString();
    }

    /**
     * 拼接url字符串（地址转经纬度）
     */
    private static String joinUrl2(String ak, String output, String location, String url) throws IOException {
        StringBuilder baseUrl = new StringBuilder();
        baseUrl.append(url).append("?address=").append(location).append("&output=").append(output)
                .append("&ak=").append(ak);
        return baseUrl.toString();
    }

    /**
     * 经纬度转地址
     */
    public static void Lon2city() throws ClassNotFoundException {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[*]");
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        SqlConf mysqlConf = new SqlConf();
        mysqlConf.setUrl("jdbc:mysql://localhost:3306/test");
        mysqlConf.setUrl("root");
        mysqlConf.setPassword("root");
        mysqlConf.setDriver(GlobalConstant.MYSQL_DRIVER());

        Dataset<Row> df = MySqlUtil2.getMysqlDate(spark, mysqlConf);

        JavaRDD<Row> rowRDD = df.rdd().toJavaRDD();

        JavaPairRDD<BigDecimal, BigDecimal> pairRDD = rowRDD.mapToPair(new PairFunction<Row, BigDecimal, BigDecimal>() {
            @Override
            public Tuple2 call(Row row) throws Exception {
                return new Tuple2<BigDecimal, BigDecimal>(row.getDecimal(10), row.getDecimal(11));
            }
        });
        List<Tuple2<BigDecimal, BigDecimal>> collect = pairRDD.collect();

        for (Tuple2<BigDecimal, BigDecimal> tuple2 : collect) {
            // 把84坐标转为百度坐标

            String location = GPS84ToBD09(tuple2._1.doubleValue(), tuple2._2.doubleValue());
            System.out.println("location:--------------" + location);
            Map<String, String> cityByLonLat = getCityByLonLat(location);
            String province = cityByLonLat.get("province");
            String city = cityByLonLat.get("city");
            String district = cityByLonLat.get("district");
            String township = cityByLonLat.get("sematic_description");
            String gjdm = cityByLonLat.get("gjdm");
            Integer domestic = 0;
            if (Strings.isBlank(cityByLonLat.get("country_code"))) {
                domestic = 0;
            } else {
                domestic = Integer.valueOf(cityByLonLat.get("country_code"));
            }

            String addr = province + city + district + township;

            NutDao dao = getDao();
            Record record = new Record();
            record.put("addr", addr);
            record.put("rzsjaddr", addr);
            record.put("province", province);
            record.put("city", city);
            record.put("district", district);
            record.put("gjdm", gjdm);
            record.put("domestic", domestic);
            dao.update("rj_dic_jz_34gjzxx", record.toChain(), Cnd.where("lat", "=", tuple2._1).and("lon", "=", tuple2._2));
        }
    }


    /**
     * 地址转经纬度
     */
    public static void city2Lon() throws ClassNotFoundException {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[*]");
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        SqlConf mysqlConf = new SqlConf();
        mysqlConf.setUrl("jdbc:mysql://localhost:3306/test");
        mysqlConf.setUrl("root");
        mysqlConf.setPassword("root");
        mysqlConf.setDriver(GlobalConstant.MYSQL_DRIVER());

        Dataset<Row> df = MySqlUtil2.getMysqlDate(spark, mysqlConf);

        JavaRDD<Row> rowRDD = df.rdd().toJavaRDD();

        // 酒店
        JavaRDD<Tuple3<String, String, String>> mapRDD = rowRDD.map(new Function<Row, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> call(Row row) throws Exception {
                return new Tuple3<String, String, String>(row.getString(0), row.getString(8), row.getString(47));
            }
        });

        List<Tuple3<String, String, String>> collect = mapRDD.collect();
        for (Tuple3<String, String, String> tuple3 : collect) {
            String local = "云南省" + tuple3._2() + tuple3._3();
            if (local.length() > 42) {
                local = local.substring(local.length() - 42);
            }
            local = local.trim().replaceAll(" ", "").replaceAll(",", "")
                    .replaceAll("\r", "").replaceAll("\n", "")
                    .replaceAll("\r\n", "").replaceAll("\u007F", "")
                    .replaceAll("，", "").replaceAll("#", "");
            Map<String, Double> cityByLonLat = getLonLatByCity(local);
            NutDao dao = getDao();
            Record record = new Record();
            record.put("lat", cityByLonLat.get("lng"));
            record.put("lng", cityByLonLat.get("lat"));
            dao.update("dwb_dm_lgxx_202001161013", record.toChain(), Cnd.where("id", "=", tuple3._1()));

//        List<Tuple2<String, String>> collect = mapRDD.collect();
//        for (Tuple2<String, String> tuple2 : collect) {
//            String local = tuple2._2;
////            if (local.length() > 42) {
////                local = local.substring(local.length() - 42);
////            }
//            local = local.trim().replaceAll(" ", "").replaceAll(",", "")
//                    .replaceAll("\r", "").replaceAll("\n", "")
//                    .replaceAll("\r\n", "").replaceAll("\u007F", "");
//            Map<String, Double> cityByLonLat = getLonLatByCity(local);
//            NutDao dao = getDao();
//            Record record = new Record();
//            record.put("lat", cityByLonLat.get("lat"));
//            record.put("lng", cityByLonLat.get("lng"));
//            dao.update("pcs", record.toChain(), Cnd.where("pg_pkid", "=", tuple2._1));
        }
    }

    /**
     * 获取nutzDao对象
     */
    public static NutDao getDao() throws ClassNotFoundException {
        NutDao dao = new NutDao();
        SimpleDataSource ds = new SimpleDataSource();
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setJdbcUrl("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull");
        ds.setUsername("root");
        ds.setPassword("root");
        dao.setDataSource(ds);
        return dao;
    }

    /**
     * 对已经用完的ak进行切换（经纬度转地址）
     */
    public static JSONObject test(JSONObject result, Map<String, String> params, String location, String url) throws IOException {
        try {
            result = JSONObject.parseObject(String.valueOf(JSONObject.parseObject(String.valueOf(JSONObject.parseObject(HttpClientUtils.doGet(url))))));
            String status = result.getString("status");
            if (status.equals("302")) {
                index++;
                params.remove("ak");
                params.put("ak", arr[index]);
                System.out.println("切换key：" + arr[index]);
                url = joinUrl(params, OUTPUT, location, GET_ADDRESS_URL);
                System.out.println(url);
            }
            result = JSONObject.parseObject(JSONObject.parseObject(JSONObject.parseObject(HttpClientUtils.doGet(url)).
                    getString("result")).getString("addressComponent"));
        } catch (Exception e) {
            index++;
            params.remove("ak");
            params.put("ak", arr[index]);
            System.out.println("切换key：" + arr[index]);
            result = JSONObject.parseObject(JSONObject.parseObject(JSONObject.parseObject(HttpClientUtils.doGet(url)).
                    getString("result")).getString("addressComponent"));
        }
        return result;
    }

    /**
     * 对已经用完的ak进行切换（地址转经纬度）
     */
    public static JSONObject test1(JSONObject result, Map<String, String> params, String location, String url) throws IOException {
        try {
            result = JSONObject.parseObject(String.valueOf(JSONObject.parseObject(String.valueOf(JSONObject.parseObject(HttpClientUtils.doGet(url))))));
            String status = result.getString("status");
            if (status.equals("1")) {
                result.put("lng", 0.000000000);
                result.put("lat", 0.000000000);
            } else {
                result = JSONObject.parseObject(JSONObject.parseObject(JSONObject.parseObject(HttpClientUtils.doGet(url)).
                        getString("result")).getString("location"));
            }
        } catch (Exception e) {
            e.printStackTrace();
            index++;
            System.out.println("切换ak之前的index: " + index);
            String ak = arr[index];
            System.out.println("切换key：" + ak);
            url = joinUrl2(ak, OUTPUT, location, GET_LNG_LAT_URL);
            System.out.println(url);

            result = JSONObject.parseObject(JSONObject.parseObject(JSONObject.parseObject(HttpClientUtils.doGet(url)).
                    getString("result")).getString("location"));
        }
        return result;
    }

    // 百度转84
    public static Map<String, Double> BD09ToGPS84(double bd_lon, double bd_lat) {
        Map<String, Double> gcj02 = BD09ToGCJ02(bd_lon, bd_lat);
        System.out.println("百度转火星" + gcj02);
        Map<String, Double> toGPS84 = GCJ02ToGPS84(gcj02.get("lon"), gcj02.get("lat"));
        return toGPS84;
    }

    // 百度转火星
    public static Map<String, Double> BD09ToGCJ02(double bd_lon, double bd_lat) {
        Map<String, Double> map = new HashMap<>();
        double x = bd_lon - 0.0065, y = bd_lat - 0.006;
        double z = Math.sqrt(x * x + y * y) - 0.00002 * Math.sin(y * pi);
        double theta = Math.atan2(y, x) - 0.000003 * Math.cos(x * pi);
        double gg_lon = z * Math.cos(theta);
        double gg_lat = z * Math.sin(theta);
        map.put("lat", gg_lat);
        map.put("lon", gg_lon);
        return map;
    }

    // 火星转84
    public static Map<String, Double> GCJ02ToGPS84(double lon, double lat) {
        Map<String, Double> map = new HashMap<>();
        Map<String, Double> transform = transform(lon, lat);
        double lontitude = lon * 2 - transform.get("lon");
        double latitude = lat * 2 - transform.get("lat");
        map.put("lat", latitude);
        map.put("lon", lontitude);
        return map;
    }

    // 84转换为百度坐标
    public static String GPS84ToBD09(double lon, double lat) {
        if (outOfChina(lon, lat)) {
            return null;
        }
        double dLat = transformLat(lon - 105.0, lat - 35.0);
        double dLon = transformLon(lon - 105.0, lat - 35.0);
        double radLat = lat / 180.0 * Math.PI;
        double magic = Math.sin(radLat);
        magic = 1 - ee * magic * magic;
        double sqrtMagic = Math.sqrt(magic);
        dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * Math.PI);
        dLon = (dLon * 180.0) / (a / sqrtMagic * Math.cos(radLat) * Math.PI);
        double mgLon = lon + dLon;
        double mgLat = lat + dLat;
        double z = Math.sqrt(mgLon * mgLon + mgLat * mgLat) + 0.00002 * Math.sin(mgLat * Math.PI);
        double theta = Math.atan2(mgLat, mgLon) + 0.000003 * Math.cos(mgLon * Math.PI);
        double longitude = z * Math.cos(theta) + 0.0065;
        double latitude = z * Math.sin(theta) + 0.006;
        return latitude + "," + longitude;
    }

    // 坐标转换相关算法
    private static boolean outOfChina(double lon, double lat) {
        if (lon < 72.004 || lon > 137.8347)
            return true;
        return lat < 0.8293 || lat > 55.8271;
    }

    private static Map<String, Double> transform(double lon, double lat) {
        Map<String, Double> map = null;
        if (outOfChina(lon, lat)) {
            map = new HashMap<>();
            return map;
        }
        double dLat = transformLat(lon - 105.0, lat - 35.0);
        double dLon = transformLon(lon - 105.0, lat - 35.0);
        double radLat = lat / 180.0 * pi;
        double magic = Math.sin(radLat);
        magic = 1 - ee * magic * magic;
        double sqrtMagic = Math.sqrt(magic);
        dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * pi);
        dLon = (dLon * 180.0) / (a / sqrtMagic * Math.cos(radLat) * pi);
        double mgLat = lat + dLat;
        double mgLon = lon + dLon;
        map = new HashMap<>();
        map.put("lat", mgLat);
        map.put("lon", mgLon);
        return map;
    }

    private static double transformLat(double x, double y) {
        double ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y
                + 0.2 * Math.sqrt(Math.abs(x));
        ret += (20.0 * Math.sin(6.0 * x * pi) + 20.0 * Math.sin(2.0 * x * pi)) * 2.0 / 3.0;
        ret += (20.0 * Math.sin(y * pi) + 40.0 * Math.sin(y / 3.0 * pi)) * 2.0 / 3.0;
        ret += (160.0 * Math.sin(y / 12.0 * pi) + 320 * Math.sin(y * pi / 30.0)) * 2.0 / 3.0;
        return ret;
    }

    private static double transformLon(double x, double y) {
        double ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1
                * Math.sqrt(Math.abs(x));
        ret += (20.0 * Math.sin(6.0 * x * pi) + 20.0 * Math.sin(2.0 * x * pi)) * 2.0 / 3.0;
        ret += (20.0 * Math.sin(x * pi) + 40.0 * Math.sin(x / 3.0 * pi)) * 2.0 / 3.0;
        ret += (150.0 * Math.sin(x / 12.0 * pi) + 300.0 * Math.sin(x / 30.0
                * pi)) * 2.0 / 3.0;
        return ret;
    }
}
