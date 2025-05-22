package com.osman.PairRDDs;

import com.osman.SparkUtil.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ReadTextFile {

    public static Set<String> boringWords = new HashSet<>();

    static{

        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader("src/main/resources/subtitles/boringwords.txt"));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("failed to read boring words file");
        }

        br.lines().forEach(boringWords::add);

    }

    public static boolean isBoring(String word) {
        return boringWords.contains(word);
    }


    public static void main(String[] args) {

        System.setProperty("org.hadoop.home.dir", "C:\\hadoop");

        JavaSparkContext sc= SparkUtils.getContext();
        JavaRDD<String> inputDataRDD = sc.textFile("src/main/resources/subtitles/input.txt");

//        inputDataRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).foreach(
//                word -> {
//                    System.out.println(word);
//                }
//        );

        JavaRDD<String> inputDataRDD1=sc.textFile("src/main/resources/subtitles/input.txt");
        JavaRDD<String> inputDataRDD2=sc.textFile("src/main/resources/subtitles/input-spring.txt");


        inputDataRDD2.map(line -> line.replaceAll("[^a-zA-Z\\s]", "")
                .toLowerCase())
                .filter(line -> !line.trim().isEmpty())
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .filter(word -> !word.trim().isEmpty() && !isBoring(word))
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(t -> new Tuple2<>(t._2(), t._1()))
                .sortByKey(false)
                .take(10)
                .forEach(val -> System.out.println(val._1() + " : " + val._2()));


        SparkUtils.closeContext();

    }
}
