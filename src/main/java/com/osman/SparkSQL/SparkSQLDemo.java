package com.osman.SparkSQL;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import scala.collection.immutable.Seq;

import static org.apache.spark.sql.functions.col;

public class SparkSQLDemo {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark SQL Demo")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///C:/temp/")
                .getOrCreate();

//        Dataset<Row> csv = spark.read().csv("src/main/resources/exams/students.csv");

        Dataset<Row> csv=spark.read().option("header", "true")
                .csv("src/main/resources/exams/students.csv");

//        csv.show();

        System.out.println("total records "+csv.count());

        Row first = csv.first();

//        String subject = first.get(2).toString()
        String subject = first.getAs("subject");
//        first.get(2);
        System.out.println("Subject: " + subject);

//        Dataset<Row> filteredMathRecords = csv.filter("subject = 'Maths'");
        Dataset<Row> filteredMathRecords = csv.filter("subject = 'Math' AND year = '2007'");
        filteredMathRecords.show();

        Dataset<Row> lambdaFiltered = csv.filter((FilterFunction<Row>) row -> "Math".equals(row.getAs("subject")) && "2007".equals(row.getAs("year")));
        //if not type cast to FilterFunction<Row> it will not work(lambda function need to be serialized)
//        lambdaFiltered.show();


//        Column yearCol = functions.col("year");
        Column yearCol = col("year");
        Column subjectCol = csv.col("subject");

//        Dataset<Row> filteredMathRecords2 = csv.filter(subjectCol.equalTo("Math").and(yearCol.equalTo("2007")));
//        filteredMathRecords2.show();

        Dataset<Row> filteredMathRecords2 = csv.filter(col("subject").equalTo("Math").and(col("year").equalTo("2007")));
        filteredMathRecords2.show();

        spark.close();

    }

}
