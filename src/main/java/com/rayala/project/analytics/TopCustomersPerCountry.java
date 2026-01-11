package com.rayala.project.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TopCustomersPerCountry {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Top Customers Per Country")
                .master("local[*]")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
                .getOrCreate();

        spark.sql("USE company_db");

        Dataset<Row> result = spark.sql(
                "SELECT CustomerId, FirstName, LastName, Country, SubscriptionDate " +
                        "FROM ( " +
                        "   SELECT CustomerId, FirstName, LastName, Country, SubscriptionDate, " +
                        "          ROW_NUMBER() OVER (PARTITION BY Country ORDER BY SubscriptionDate DESC) AS rn " +
                        "   FROM customers_bucketed " +
                        ") t " +
                        "WHERE rn <= 10"
        );

        result.show(false);

        spark.stop();
    }
}
