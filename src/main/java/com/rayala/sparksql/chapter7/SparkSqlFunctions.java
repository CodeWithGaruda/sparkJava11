package com.rayala.sparksql.chapter7;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlFunctions {
    static void stringFunctions(SparkSession spark) {
        System.out.println("---- String Functions ----");

        spark.sql(
                "SELECT name, " +
                        "UPPER(name) AS upper_name, " +
                        "LOWER(city) AS lower_city, " +
                        "LENGTH(name) AS name_length, " +
                        "SUBSTRING(name, 1, 3) AS short_name " +
                        "FROM employees"
        ).show();
    }

    static void numericFunctions(SparkSession spark) {
        System.out.println("---- Numeric Functions ----");

        spark.sql(
                "SELECT name, salary, " +
                        "salary * 1.10 AS increased_salary, " +
                        "ROUND(salary / 12, 2) AS monthly_salary " +
                        "FROM employees"
        ).show();
    }

    static void nullHandlingFunctions(SparkSession spark) {
        System.out.println("---- Null Handling ----");

        /*
        | Function    | Meaning              |
        | ----------- | -------------------- |
        | COALESCE    | first non-null value |
        | IFNULL      | replace null         |
        | IS NULL     | check null           |
        | IS NOT NULL | check not null       |
         */

        spark.sql(
                "SELECT name, " +
                        "salary, " +
                        "COALESCE(salary, 0) AS salary_filled, " +
                        "IFNULL(department, 'UNKNOWN') AS dept_filled " +
                        "FROM employees"
        ).show();
    }

    static void dateFunctions(SparkSession spark) {
        System.out.println("---- Date Functions ----");

        spark.sql(
                "SELECT name, joining_date, " +
                        "YEAR(joining_date) AS join_year, " +
                        "MONTH(joining_date) AS join_month, " +
                        "DATEDIFF(CURRENT_DATE(), joining_date) AS days_with_company " +
                        "FROM employees"
        ).show();
    }

    static void caseWhenExample(SparkSession spark) {
        System.out.println("---- CASE WHEN ----");

        spark.sql(
                "SELECT name, salary, " +
                        "CASE " +
                        "WHEN salary >= 70000 THEN 'HIGH' " +
                        "WHEN salary >= 50000 THEN 'MEDIUM' " +
                        "ELSE 'LOW' " +
                        "END AS salary_band " +
                        "FROM employees"
        ).show();
    }


    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("SparkSqlFunctions")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> emp = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/empFunctions.csv");

        emp.createOrReplaceTempView("employees");

        stringFunctions(spark);
        numericFunctions(spark);
        nullHandlingFunctions(spark);
        dateFunctions(spark);
        caseWhenExample(spark);

        spark.stop();
    }
}