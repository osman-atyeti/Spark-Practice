package com.osman.tuple;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

public class TupleDemo {
    public static void main(String[] args) {

        Tuple2<String, Integer> tuple = new Tuple2<>("Hello", 42);
        Tuple3<String,String,String> tuple3 = new Tuple3<>("Hello", "World", "Java");

//        new Tuple5<>("Hello", 1, 2, 3, 4);

        String firstElement = tuple._1();
        Integer secondElement = tuple._2();

        System.out.println("First element: " + firstElement);
        System.out.println("Second element: " + secondElement);

    }
}
