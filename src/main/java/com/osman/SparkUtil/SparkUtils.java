package com.osman.SparkUtil;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkUtils {
    private static final SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    private static final JavaSparkContext sc = new JavaSparkContext(conf);

    public static JavaSparkContext getContext() {
        return sc;
    }

    public static void closeContext() {
        sc.close();
    }
}
