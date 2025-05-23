package com.osman.SparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLDemo {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark SQL Demo")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///C:/temp/")
                .getOrCreate();

        Dataset<Row> csv = spark.read().csv("src/main/resources/exams/students.csv");

        csv.show();

        spark.close();


    }

}
