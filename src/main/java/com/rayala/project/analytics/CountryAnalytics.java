package com.rayala.project.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

public class CountryAnalytics {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Country Analytics")
                .master("local[*]")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
                .getOrCreate();

        spark.sql("USE company_db");

        Dataset<Row> result = spark.sql(
                "SELECT Country, COUNT(*) AS total_customers " +
                        "FROM customers_bucketed " +
                        "GROUP BY Country " +
                        "ORDER BY total_customers DESC " +
                        "LIMIT 10"
        );

        result.show(false);

        // Show how Spark optimized it
        spark.sql(
                "EXPLAIN SELECT Country, COUNT(*) " +
                        "FROM customers_bucketed " +
                        "GROUP BY Country"
        ).show(false);

        int n=(new Scanner(System.in)).nextInt();
        spark.stop();
    }
}
