package com.osman.SparkSQL;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class InMemoryDemo {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        SparkSession spark = SparkSession.builder()
                .appName("In Memory Demo")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///C:/temp/")
                .getOrCreate();
        // Create an in-memory dataset
        List<Row> inMemory= Arrays.asList(
                RowFactory.create(1, "Alice", 29),
                RowFactory.create(2, "Bob", 31),
                RowFactory.create(3, "Cathy", 25)
        );

        StructField[] fields=new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty())
        };

        StructType schema = new StructType(fields);
        Dataset<Row> inMemoryDF = spark.createDataFrame(inMemory, schema);
        inMemoryDF.show();

        spark.stop();
    }
}
