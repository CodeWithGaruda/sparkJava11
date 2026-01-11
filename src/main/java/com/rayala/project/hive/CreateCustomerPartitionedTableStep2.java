package com.rayala.project.hive;

import org.apache.spark.sql.SparkSession;

public class CreateCustomerPartitionedTableStep2 {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Create Partitioned Table")
                .master("local[*]")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
                .getOrCreate();

        spark.sql("USE company_db");

        spark.sql("DROP TABLE IF EXISTS customers_part");

        spark.sql(
                "CREATE TABLE customers_part (" +
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
                        "STORED AS PARQUET"
        );

        System.out.println("Partitioned table created");
        spark.stop();
    }
}
