package com.rayala.project.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MonthlySubscriptionTrend {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Monthly Subscription Trend")
                .master("local[*]")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
                .getOrCreate();

        spark.sql("USE company_db");

        Dataset<Row> result = spark.sql(
                "SELECT substr(SubscriptionDate, 1, 7) AS month, " +
                        "       COUNT(*) AS total_customers " +
                        "FROM customers_bucketed " +
                        "GROUP BY substr(SubscriptionDate, 1, 7) " +
                        "ORDER BY month"
        );

        result.show(false);

        spark.stop();
    }
}
