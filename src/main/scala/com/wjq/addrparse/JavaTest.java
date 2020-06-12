package com.wjq.addrparse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class JavaTest {
    public static void main(String[] args) {
        // select * into outfile 'filename.txt' from tablname;
        // 1、把整个数据库导出为一个sql文件:用：mysqldump dbname > c:\mydb.sql
        /***
         * SELECT * FROM passwd INTO OUTFILE '/tmp/runoob.txt'
         *     -> FIELDS TERMINATED BY ',' ENCLOSED BY '"'
         *     -> LINES TERMINATED BY '\r\n';
         */
        ArrayList<Double> list = new ArrayList<Double>();

        list.add(10.1999);
        list.add(10.12);
        list.add(19.0181);
        System.out.println(list.toString());
        Collections.sort(list);
        System.out.println(list.toString());
        System.out.println(list.get(0));

    }
}
