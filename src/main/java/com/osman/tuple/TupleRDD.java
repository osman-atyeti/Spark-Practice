package com.osman.tuple;

import com.osman.Main;
import com.osman.SparkUtil.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TupleRDD {

    public static void main(String[] args) {

        List<Integer> inputData = new ArrayList<>();
        inputData.add(10);
        inputData.add(22);
        inputData.add(31);
        inputData.add(43);

        JavaSparkContext sc=SparkUtils.getContext();

        JavaRDD<Integer> inputRDD = sc.parallelize(inputData);

        JavaRDD<Tuple2<Integer,Double>> pairRdd= inputRDD.map(val -> new Tuple2<>(val,Math.sqrt(val)));

        pairRdd.foreach(tuple -> {
            System.out.println("Value: " + tuple._1() + ", Square Root: " + tuple._2());
        });

        SparkUtils.closeContext();

    }

}
