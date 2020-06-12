package com.wjq.xsd;

public class Similarity {
    public static final  String content1="   ";

    public static final  String content2=" ";




    public static void main(String[] args) {

        double  score=CosineSimilarity.getSimilarity(content1,content2);
        System.out.println("相似度："+score);

        score=CosineSimilarity.getSimilarity(content1,content1);
        System.out.println("相似度："+score);
    }
}
