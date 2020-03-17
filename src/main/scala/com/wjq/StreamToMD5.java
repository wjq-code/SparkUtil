package com.wjq;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class StreamToMD5 {
    private char[] hexDigits = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
    public String stream2Md5(InputStream is){
        // 如果照片大小为空则返回null
        try {
            if (is.read() == -1) {
                return null;
            }else{
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                byte[] buffer = new byte[1024];
                int numRead = 0;
                while((numRead = is.read(buffer)) > 0){
                    md5.update(buffer,0,numRead);
                }
//                return buffer2Hex();
            }

        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String buffer2Hex(byte bt, StringBuffer sb) {
        char c0 = hexDigits[((bt & 0xF0) >> 4)];
        char c1 = hexDigits[(bt & 0xF)];


        return null;
    }

    private static String getMd5(InputStream is) throws IOException {
        String md5 = DigestUtils.md2Hex(is);
        return md5;
    }
}
