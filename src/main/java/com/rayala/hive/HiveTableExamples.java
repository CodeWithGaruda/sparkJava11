package com.rayala.hive;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveTableExamples {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("HiveTableExamples")
                .master("local[*]")
                .enableHiveSupport()
                .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
                .getOrCreate();

        // Read CSV once
        Dataset<Row> empDF = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/emp.csv");

        spark.sql("CREATE DATABASE IF NOT EXISTS company_db");
        spark.sql("USE company_db");

        createManagedTable(spark, empDF);
        createExternalTable(spark, empDF);

        spark.stop();
    }

    /**
     * Creates a HIVE MANAGED table.
     *
     * - Hive owns the data
     * - Data is stored inside spark-warehouse
     * - Dropping table deletes data
     */
    private static void createManagedTable(SparkSession spark, Dataset<Row> empDF) {

        // Always drop first to avoid location-exists error
        spark.sql("DROP TABLE IF EXISTS employees_managed");

        empDF.write()
                .mode("overwrite")
                .saveAsTable("employees_managed");

        System.out.println("Managed table data:");
        spark.sql("SELECT * FROM employees_managed").show();
    }

    /**
     * Creates a HIVE EXTERNAL table.
     *
     * - Hive does NOT own the data
     * - Data stored at custom location
     * - Dropping table keeps data
     */
    private static void createExternalTable(SparkSession spark, Dataset<Row> empDF) {

        spark.sql("DROP TABLE IF EXISTS employees_external");

        empDF.write()
                .mode("overwrite")
                .option("path", "file:///C:/external-data/employees")
                .saveAsTable("employees_external");

        System.out.println("External table data:");
        spark.sql("SELECT * FROM employees_external").show();
    }
}
