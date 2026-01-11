package com.rayala.project.hive;

import org.apache.spark.sql.SparkSession;

public class LoadCustomersIntoPartitionsStep3 {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Load Partitioned Data")
                .master("local[*]")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
                .getOrCreate();

        spark.sql("USE company_db");

        spark.sql(
                "INSERT OVERWRITE TABLE customers_part PARTITION (Country) " +
                        "SELECT " +
                        "Index, CustomerId, FirstName, LastName, Company, City, " +
                        "Phone1, Phone2, Email, SubscriptionDate, Website, Country " +
                        "FROM raw_customers"
        );

        System.out.println("Data loaded into partitions");

        spark.sql("SHOW PARTITIONS customers_part").show(false);

        /*
            SELECT COUNT(*) FROM customers_part;
            SELECT COUNT(*) FROM customers_part WHERE Country = 'India';
         */

        spark.sql("SELECT COUNT(*) FROM customers_part").show(false);
        spark.sql("SELECT COUNT(*) as India_Count  FROM customers_part WHERE Country = 'India'").show(false);

        spark.stop();
    }
}
