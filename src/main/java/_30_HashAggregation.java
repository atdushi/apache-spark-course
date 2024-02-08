package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir", "c:/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

//        spark.conf().set("spark.sql.shuffle.partitions", "12");

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

        dataset.createOrReplaceTempView("logging_table");

        //-------------SortAggregate------------------
//        Dataset<Row> results = spark.sql
//                ("select level, date_format(datetime, 'MMMM') as month, count(1) as total, first(date_format(datetime, 'M')) as monthnum " +
//                        "from logging_table group by level, month order by cast(monthnum as int), level");

        //-------------HashAggregate------------------
        //cast monthnum to int
//        Dataset<Row> results = spark.sql
//                ("select level, date_format(datetime, 'MMMM') as month, count(1) as total, first(cast(date_format(datetime, 'M') as int)) as monthnum " +
//                        "from logging_table group by level, month order by monthnum, level");

        //or group by mounthnum
        Dataset<Row> results = spark.sql
                ("select level, date_format(datetime, 'MMMM') as month, count(1) as total, date_format(datetime, 'M') as monthnum " +
                        "from logging_table group by level, month, monthnum order by monthnum, level");

        results.show(100);

        results.explain();

        //-------------HashAggregate------------------
//        dataset = dataset.select(col("level"),
//                date_format(col("datetime"), "MMMM").alias("month"),
//                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));
//
//        dataset = dataset.groupBy("level", "month", "monthnum")
//                .count().as("total")
//                .orderBy("monthnum");
//
//         = dataset.drop("monthnum");
//
//        dataset.show(100);
//
//        dataset.explain();

        spark.close();
    }


}

