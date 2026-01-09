package com.rayala.hive.partition;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HivePartitionExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("HivePartitionExample")
                .master("local[*]")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
                .config("javax.jdo.option.ConnectionURL",
                        "jdbc:derby:;databaseName=C:/spark-metastore/metastore_db;create=true")
                .getOrCreate();

        Dataset<Row> empDF = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/emp_big.csv");

        spark.sql("CREATE DATABASE IF NOT EXISTS company_db");
        spark.sql("USE company_db");

        spark.sql("DROP TABLE IF EXISTS employees_part");

        empDF.write()
                .mode("overwrite")
                .partitionBy("department")
                .saveAsTable("employees_part");

        spark.sql("SELECT * FROM employees_part").show();

        spark.stop();
    }
}

