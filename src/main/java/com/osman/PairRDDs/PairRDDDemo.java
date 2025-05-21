package com.osman.PairRDDs;

import com.google.common.collect.Iterables;
import com.osman.SparkUtil.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PairRDDDemo {
    private static List<String> inputData;

    public static void main(String[] args) {

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");


        JavaSparkContext sc= SparkUtils.getContext();

        JavaRDD<String> inputDataRDD = sc.parallelize(inputData);

        JavaPairRDD<String,String> pairRDD = inputDataRDD.mapToPair(line -> {
            String[] parts = line.split(":");
            String level = parts[0];
            String msg= parts[1];
            return new Tuple2<>(level, msg);
        });

//        pairRDD.foreach(tuple -> {
//            System.out.println("Key: " + tuple._1() + ", Value: " + tuple._2());
//        });
//
//        pairRDD.groupByKey()
//                .foreach(val -> System.out.println(val._1() + " : " + Iterables.size(val._2())));


        pairRDD.mapToPair(val -> new Tuple2<>(val._1(),1))
                        .reduceByKey(Integer::sum)
                        .mapToPair(t -> new Tuple2<>(t._2(), t._1()))
                        .foreach(val -> System.out.println(val._1() + " : " + val._2()));


        SparkUtils.closeContext();

    }
}
