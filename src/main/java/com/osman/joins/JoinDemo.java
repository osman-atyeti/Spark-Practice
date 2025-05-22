package com.osman.joins;

import com.osman.SparkUtil.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class JoinDemo {

    public static void main(String[] args) {

        JavaSparkContext sc=SparkUtils.getContext();

        List<Tuple2<Integer,Integer>> visits= new ArrayList<>();
        visits.add(new Tuple2<>(4,18));
        visits.add(new Tuple2<>(6,4));
        visits.add(new Tuple2<>(10,9));

        List<Tuple2<Integer,String>> users= new ArrayList<>();
        users.add(new Tuple2<>(1,"John"));
        users.add(new Tuple2<>(2,"Jane"));
        users.add(new Tuple2<>(3,"Doe"));
        users.add(new Tuple2<>(4,"Osman"));
        users.add(new Tuple2<>(5,"Marielle"));
        users.add(new Tuple2<>(6,"Ali"));


        JavaPairRDD<Integer,Integer> visitsRdd= sc.parallelizePairs(visits);
        JavaPairRDD<Integer,String> usersRdd= sc.parallelizePairs(users);

//        //inner join
//        JavaPairRDD<Integer,Tuple2<Integer,String>> innerJoinRdd= visitsRdd.join(usersRdd);
//        innerJoinRdd.foreach(record -> {
//            System.out.println("User id:"+record._1() + " has visited: " + record._2._1() + " times and name is: " + record._2._2());
//        });
//
//        //left outer join
//        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftOuterJoinRdd = visitsRdd.leftOuterJoin(usersRdd);
//        leftOuterJoinRdd.foreach(record -> {
//            System.out.println("User id:" + record._1() + " has visited: " + record._2._1() + " times and name is: " + record._2._2().orElse("Unknown"));
//        });
//
//        //right outer join
//        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightOuterJoinRdd = visitsRdd.rightOuterJoin(usersRdd);
//        rightOuterJoinRdd.foreach(record -> {
//            System.out.println("User id:" + record._1() + " has visited: " + record._2._1().orElse(0) + " times and name is: " + record._2._2());
//        });
//
//        //full outer join
//        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullOuterJoinRdd = visitsRdd.fullOuterJoin(usersRdd);
//        fullOuterJoinRdd.foreach(record -> {
//            System.out.println("User id:" + record._1() + " has visited: " + record._2._1().orElse(0) + " times and name is: " + record._2._2().orElse("Unknown"));
//        });

        //cross join
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> crossJoinRdd = visitsRdd.cartesian(usersRdd);
        crossJoinRdd.foreach(record -> {
            System.out.println("User id:" + record._1._1() + " has visited: " + record._1._2() + " times and name is: " + record._2._2());
        });



        SparkUtils.closeContext();

    }

}
