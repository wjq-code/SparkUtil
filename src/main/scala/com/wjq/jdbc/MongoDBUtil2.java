package com.wjq.jdbc;

import com.google.common.primitives.Bytes;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.beans.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.Map.Entry;

public class MongoDBUtil2 {

    public static Document BeanToDoc(Object obj) throws IllegalAccessException, InvocationTargetException, IntrospectionException {
        Map<String, Object> map = objectToMap(obj);

        if (map.get("_id") != null) {
            map.put("_id", new ObjectId(map.get("_id").toString()));
        }

        return new Document(map);
    }

//    public static Object DocToBean(Document doc, String CollectionName) throws Exception {
//
//        if (doc.get("_id") != null) {
//            doc.put("_id", doc.get("_id").toString());
//        }
//
//        Object obj = mapToBean(doc, MongoDbCollectionBlue.blue(CollectionName));
//        return obj;
//    }

//    @SuppressWarnings("unchecked")
//    public static <T> Object mapToBean(Map<String, Object> map, Class<T> T) throws Exception {
//
//        // 实例化bean
//        T bean = T.newInstance();
//
//        //调用mongodb定义的vo对象转换方法
//        Method change = T.getDeclaredMethod("change", Map.class);
//        change.invoke(bean, map);
//
//
//        // 获取这个类的所有属性
//        Field[] propertys = T.getDeclaredFields();
//
//        // 遍历属性集获取单个属性
//        for (Field property : propertys) {
//
//            // 获取属性申明的名字
//            String propertyName = property.getName();
//            // 判断map中是否有这个属性的定义的名字
//            if (map.keySet().contains(propertyName)) {
//
//                // 获取到属性的值value
//                Object value = map.get(propertyName);
//
//                // 未知
//                property.setAccessible(true);
//                // 获取属性类型
//                Class<?> propertyType = property.getType();
//                see(propertyType, "propertyType=");
//
//                // 属性类型分辨
//                int switchPathByPropertyType = switchPathByClassType(propertyType);
//
//                switch (switchPathByPropertyType) {
//                    case 1:
//                        // 基础数据类型
//                        BeanUtils.copyProperty(bean, propertyName, value);
//                        break;
//                    case 2:
//                        // Collection
//                        // 得到泛型里的class类型对象
//                        // Collection类型
//                        // 获取genericType
//                        Type genericType = property.getGenericType();
//                        if (genericType == null)
//                            continue;
//                        ParameterizedType pt = (ParameterizedType) genericType;
//                        Class<?> collectionValueClass = (Class<?>) pt.getActualTypeArguments()[0];
//
//                        int switchPathByCollectionValueClass = switchPathByClassType(collectionValueClass);
//                        // 强转成为Collection，用于遍历
//                        Collection<Object> collection = (Collection<Object>) value;
//
//                        // 实例化泛型参数强转为Collection，用于接收值
//                        Collection<Object> propertyBean = (Collection<Object>) propertyType.newInstance();
//
//                        // 遍历，递归将value中的所有map转换为bean
//                        for (Object collectionValue : collection) {
//                            switch (switchPathByCollectionValueClass) {
//                                case 1:
//                                    // 基础数据类型
//                                    propertyBean.add(collectionValue);// 直接添加
//                                    break;
//                                case 2:
//                                    // Collection
//                                    break;
//                                case 3:
//                                    // Map
//                                    Object collectionValueBean = mapToBean((Map<String, Object>) collectionValue, collectionValueClass);
//                                    propertyBean.add(collectionValueBean);
//                                    break;
//                            }
//                        }
//
//                        BeanUtils.copyProperty(bean, propertyName, propertyBean);
//
//                        break;
//                    case 3:
//                        // Map
//                        BeanUtils.copyProperty(bean, propertyName, mapToBean((Map<String, Object>) value, propertyType));
//                        break;
//                }
//            }
//        }
//        return bean;
//    }

//    public static int switchPathByClassType(Class<?> classType) {
//        int switchPath = 0;
//        // 基本数据类型String,Boolean,Byte,Short,Integer,Long,Float,Double,Enum
//        if (classType.isPrimitive() || classType == byte[].class || classType == String.class || classType == Boolean.class || classType == Byte.class || classType == Short.class || classType == Integer.class || classType == Long.class || classType == Float.class || classType == Double.class || classType == Enum.class) {
//            switchPath = 1;
//        } else if (Collection.class.isAssignableFrom(classType)) {
//            switchPath = 2;
//        } else {
//            switchPath = 3;
//        }
//        return switchPath;
//    }

    public static void see(Class<?> T, String memo) throws IntrospectionException {
        // 获取属性的类型的描述
        BeanInfo propertyBeanInfo = Introspector.getBeanInfo(T);
        BeanDescriptor propertyBeanDescriptor = propertyBeanInfo.getBeanDescriptor();
        // 获取到属性的类型的名称
        String propertyTypeName = propertyBeanDescriptor.getDisplayName();
        // 判断是否为基本类型—String、int、float
        // String,Boolean,Byte,Short,Integer,Long,Float,Double
    }

    public static Map<String, Object> objectToMap(Object obj) {
        try {
            Class type = obj.getClass();
            Map returnMap = new HashMap();
            BeanInfo beanInfo = Introspector.getBeanInfo(type);

            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
            for (int i = 0; i < propertyDescriptors.length; i++) {
                PropertyDescriptor descriptor = propertyDescriptors[i];
                String propertyName = descriptor.getName();

                if (!propertyName.equals("class")) {
                    Method readMethod = descriptor.getReadMethod();

                    if (readMethod == null) {
                        continue;
                    }

                    Object result = readMethod.invoke(obj, new Object[0]);

                    if (result == null) {
                        continue;
                    }

                    // 判断是否为 基础类型
                    // String,Boolean,Byte,Short,Integer,Long,Float,Double
                    // 判断是否集合类，COLLECTION,MAP
                    if (result instanceof String || result instanceof Boolean || result instanceof Byte || result instanceof Short || result instanceof Integer || result instanceof Long || result instanceof Float || result instanceof Double) {
                        if (result != null) {
                            returnMap.put(propertyName, result);
                        }
                    } else if (result instanceof Collection) {
                        Collection<?> lstObj = arrayToMap((Collection<?>) result);
                        returnMap.put(propertyName, lstObj);
                    } else if (result instanceof Map) {
                        Map<Object, Object> lstObj = mapToMap((Map<Object, Object>) result);
                        returnMap.put(propertyName, lstObj);

                    } else if (result instanceof byte[]) {
                        //数组类型
                        List<Byte> list = Bytes.asList((byte[]) result);

                        returnMap.put(propertyName, list);
                    } else if (result instanceof Enum) {
                        //枚举类型
                        returnMap.put(propertyName, result.toString());
                    } else {

                        Map mapResult = objectToMap(result);
                        returnMap.put(propertyName, mapResult);
                    }
                }
            }
            return returnMap;
        } catch (Exception e) {

            e.printStackTrace();
            return null;
        }
    }

    private static Map<Object, Object> mapToMap(Map<Object, Object> orignMap) throws IllegalAccessException, InvocationTargetException, IntrospectionException {
        Map<Object, Object> resultMap = new HashMap<Object, Object>();
        for (Entry<Object, Object> entry : orignMap.entrySet()) {
            Object key = entry.getKey();
            Object resultKey = null;
            if (key instanceof Collection) {
                resultKey = arrayToMap((Collection) key);
            } else if (key instanceof Map) {
                resultKey = mapToMap((Map) key);
            } else {
                if (key instanceof String || key instanceof Boolean || key instanceof Byte || key instanceof Short || key instanceof Integer || key instanceof Long || key instanceof Float || key instanceof Double || key instanceof Enum) {
                    if (key != null) {
                        resultKey = key;
                    }
                } else {
                    resultKey = objectToMap(key);
                }
            }

            Object value = entry.getValue();
            Object resultValue = null;
            if (value instanceof Collection) {
                resultValue = arrayToMap((Collection) value);
            } else if (value instanceof Map) {
                resultValue = mapToMap((Map) value);
            } else {
                if (value instanceof String || value instanceof Boolean || value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long || value instanceof Float || value instanceof Double || value instanceof Enum) {
                    if (value != null) {
                        resultValue = value;
                    }
                } else {
                    resultValue = objectToMap(value);
                }
            }

            resultMap.put(resultKey, resultValue);
        }
        return resultMap;
    }

    private static Collection arrayToMap(Collection lstObj) throws IllegalAccessException, InvocationTargetException, IntrospectionException {
        ArrayList arrayList = new ArrayList();

        for (Object t : lstObj) {
            if (t instanceof Collection) {
                Collection result = arrayToMap((Collection) t);
                arrayList.add(result);
            } else if (t instanceof Map) {
                Map result = mapToMap((Map) t);
                arrayList.add(result);
            } else {
                if (t instanceof String || t instanceof Boolean || t instanceof Byte || t instanceof Short || t instanceof Integer || t instanceof Long || t instanceof Float || t instanceof Double || t instanceof Enum) {
                    if (t != null) {
                        arrayList.add(t);
                    }
                } else {
                    Object result = objectToMap(t);
                    arrayList.add(result);
                }
            }
        }
        return arrayList;
    }

}