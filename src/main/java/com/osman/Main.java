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

    public static SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    public static JavaSparkContext sc =new JavaSparkContext(conf);

    public static void main(String[] args) {

        List<Double> inputData = new ArrayList<>();
        inputData.add(1.0);
        inputData.add(212.023);
        inputData.add(31.0);
        inputData.add(432.34);
        inputData.add(12.0);


        JavaRDD<Double> inputDataRdd=sc.parallelize(inputData);

//        Double result= inputDataRdd.reduce((a, b) -> a + b);
//        System.out.println(result);
    
        JavaRDD<Double> sqrRootRdd= inputDataRdd.map(Math::sqrt);
        sqrRootRdd.collect().forEach(System.out::println); //print stream is not serializable, will throw error

        //counting elements using map and reduce
        Integer count = inputDataRdd.map(val -> 1).reduce(Integer::sum);
        System.out.println("Count: " + count);

        sc.close();

    }
}