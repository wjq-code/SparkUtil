package com.wjq.addrparse;

import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

/**
 * @author AxeLai
 * @date 2019-05-14 15:09
 */
public class Addr2Location {
    /**
     * Baidu地图通过地址获取经纬度
     */
    public static String getLngAndLat(String address) {
        String location = "";
        address = address.replace(" ", "");
        //http://api.map.baidu.com/geocoder/v2/?address="地址"&output=json&ak="你的AK"
        //http://api.map.baidu.com/reverse_geocoding/v3/?ak=你的AK&output=json&coordtype=wgs84ll&location=31.225696,121.49884
        //http://api.map.baidu.com/geocoding/v3/?address=北京市海淀区上地十街10号&output=json&ak=您的ak&callback=showLocation
//        nSxiPohfziUaCuONe4ViUP2N
        //String url = "http://api.map.baidu.com/reverse_geocoding/v3/?ak=W1yVHGUHP8sxc0I8CTUSOSVqpxiIP4nQ&output=json&coordtype=wgs84ll&location=31.225696,121.49884";
        String url = "http://api.map.baidu.com/geocoding/v3/?address=" +address+"&output=json&ak=nSxiPohfziUaCuONe4ViUP2N&callback=showLocation";
        try {
            String json = loadJSON(url);
            JSONObject obj = JSONObject.parseObject(json);
            if (obj.get("status").toString().equals("0")) {
                double lng = obj.getJSONObject("result").getJSONObject("location").getDouble("lng");
                double lat = obj.getJSONObject("result").getJSONObject("location").getDouble("lat");
//                double lng = obj.getJSONObject("location").getDouble("lng");
//                double lat = obj.getJSONObject("location").getDouble("lat");
                String addr = obj.getJSONObject("result").getJSONObject("location").getString("address");
                location = lng + "," + lat + "," + addr;
            } else {
                System.out.println("未找到相匹配的经纬度！");
            }
        } catch (Exception e) {
            System.out.println("未找到相匹配的经纬度，请检查地址！");
        }
        return location;
    }

    public static String loadJSON(String url) {
        StringBuilder json = new StringBuilder();
        try {
            URL oracle = new URL(url);
            URLConnection yc = oracle.openConnection();
            BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream(), "UTF-8"));
            String inputLine = null;
            while ((inputLine = in.readLine()) != null) {
                json.append(inputLine);
            }
            in.close();
        } catch (MalformedURLException e) {
        } catch (IOException e) {
        }
        return json.toString();
    }

    public static void main(String[] args) {
        String location = getLngAndLat("江苏省苏州吴中区晋合广场");
        System.out.println(location);
    }
}