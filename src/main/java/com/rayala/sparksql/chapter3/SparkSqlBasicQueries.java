package com.rayala.sparksql.chapter3;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlBasicQueries {
    static void selectExample(SparkSession spark) {
        System.out.println("---- SELECT Example ----");
        spark.sql(" SELECT name, department, salary FROM employees").show();
    }

    static void whereExample(SparkSession spark) {
        System.out.println("---- WHERE Example ----");
        spark.sql("SELECT name, salary FROM employees WHERE salary > 60000").show();
    }

    static void orderByExample(SparkSession spark) {
        System.out.println("---- ORDER BY Example ----");
        spark.sql(" SELECT name, salary FROM employees ORDER BY salary DESC ").show();
    }

    static void limitExample(SparkSession spark) {
        System.out.println("---- LIMIT Example ----");
        spark.sql(" SELECT name, salary FROM employees LIMIT 3 ").show();
    }

    static void combinedExample(SparkSession spark) {
        System.out.println("---- Combined SQL Example ----");
        spark.sql("SELECT name, department, salary FROM employees WHERE salary IS NOT NULL ORDER BY salary DESC LIMIT 3 ").show();
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("SparkSqlBasicQueries").master("local[*]").getOrCreate();

        Dataset<Row> emp = spark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/employees.csv");

        emp.createOrReplaceTempView("employees");

        selectExample(spark);
        whereExample(spark);
        orderByExample(spark);
        limitExample(spark);
        combinedExample(spark);

        spark.stop();
    }
}