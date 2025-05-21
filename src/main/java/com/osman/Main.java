package com.osman;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        List<Double> inputData = new ArrayList<>();
        inputData.add(1.0);
        inputData.add(212.023);
        inputData.add(31.0);
        inputData.add(432.34);
        inputData.add(12.0);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc =new JavaSparkContext(conf);

        JavaRDD<Double> inputDataRdd=sc.parallelize(inputData);

        Double result= inputDataRdd.reduce((a, b) -> a + b);


    }
}