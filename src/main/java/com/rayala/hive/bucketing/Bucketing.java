package com.rayala.hive.bucketing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Bucketing {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("BucketingExample")
                .master("local[*]")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
                .getOrCreate();

        Dataset<Row> empDF = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/emp_big.csv");

        spark.sql("CREATE DATABASE IF NOT EXISTS company_db");
        spark.sql("USE company_db");

        spark.sql("DROP TABLE IF EXISTS employees_bucketed");

        empDF.write()
                .format("parquet")
                .bucketBy(4, "department")   // 👈 bucketing
                .sortBy("department")        // optional but recommended
                .mode("overwrite")
                .saveAsTable("employees_bucketed");

        spark.sql(
                "SELECT department, COUNT(*) " +
                        "FROM employees_bucketed " +
                        "GROUP BY department"
        ).show(false);

        spark.sql(
                "EXPLAIN SELECT * FROM employees_bucketed WHERE department = 'IT'"
        ).show(false);



    }
}
