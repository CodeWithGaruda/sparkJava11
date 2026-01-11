package com.rayala.project.hive;

import org.apache.spark.sql.SparkSession;

public class LoadBucketedDataStep5 {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Load Bucketed Data")
                .master("local[*]")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
                .getOrCreate();

        spark.sql("USE company_db");

        spark.sql(
                "INSERT OVERWRITE TABLE customers_bucketed PARTITION (Country) " +
                        "SELECT " +
                        "Index, CustomerId, FirstName, LastName, Company, City, " +
                        "Phone1, Phone2, Email, SubscriptionDate, Website, Country " +
                        "FROM raw_customers"
        );

        System.out.println("Data loaded into buckets");

        //verify
        spark.sql("DESCRIBE FORMATTED customers_bucketed").show(false);
        spark.stop();
    }
}
