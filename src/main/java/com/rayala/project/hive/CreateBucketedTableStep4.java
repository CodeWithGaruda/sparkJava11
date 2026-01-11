package com.rayala.project.hive;

import org.apache.spark.sql.SparkSession;

public class CreateBucketedTableStep4 {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Create Bucketed Table")
                .master("local[*]")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
                .getOrCreate();

        spark.sql("USE company_db");

        spark.sql("DROP TABLE IF EXISTS customers_bucketed");

        spark.sql(
                "CREATE TABLE customers_bucketed (" +
                        "Index INT, " +
                        "CustomerId STRING, " +
                        "FirstName STRING, " +
                        "LastName STRING, " +
                        "Company STRING, " +
                        "City STRING, " +
                        "Phone1 STRING, " +
                        "Phone2 STRING, " +
                        "Email STRING, " +
                        "SubscriptionDate DATE, " +
                        "Website STRING" +
                        ") PARTITIONED BY (Country STRING) " +
                        "CLUSTERED BY (CustomerId) INTO 16 BUCKETS " +
                        "STORED AS PARQUET"
        );

        System.out.println("Bucketed table created");
        spark.stop();
    }
}
