package com.rayala.project.hive;

import org.apache.spark.sql.SparkSession;

public class DescribeBuckets {
    /*
        Once done previous 5 steps then we can query bucketed customers
     */
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Load Bucketed Data")
                .master("local[*]")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
                .getOrCreate();

        spark.sql("USE company_db");

        //verify
        spark.sql("DESCRIBE FORMATTED customers_bucketed").show(false);
        spark.stop();
    }
}
