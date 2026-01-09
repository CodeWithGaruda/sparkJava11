package com.rayala.sparksql.chapter4;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlAggregations {
    static void countExample(SparkSession spark) {
        System.out.println("---- COUNT Example ----");

        spark.sql(
                "SELECT COUNT(*) AS total_employees " +
                        "FROM employees"
        ).show();
    }

    static void groupByExample(SparkSession spark) {
        System.out.println("---- GROUP BY Example ----");

        spark.sql(
                "SELECT department, COUNT(*) AS emp_count " +
                        "FROM employees " +
                        "GROUP BY department"
        ).show();
    }

    static void havingExample(SparkSession spark) {
        System.out.println("---- HAVING Example ----");

        spark.sql(
                "SELECT department, AVG(salary) AS avg_salary " +
                        "FROM employees " +
                        "GROUP BY department " +
                        "HAVING AVG(salary) > 60000"
        ).show();


    }

    static void multipleAggExample(SparkSession spark) {
        System.out.println("---- Multiple Aggregations ----");

        spark.sql(
                "SELECT department, " +
                        "COUNT(*) AS emp_count, " +
                        "AVG(salary) AS avg_salary, " +
                        "MAX(salary) AS max_salary " +
                        "FROM employees " +
                        "GROUP BY department"
        ).show();
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("SparkSqlAggregations")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> emp = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/employees.csv");

        emp.createOrReplaceTempView("employees");

        countExample(spark);
        groupByExample(spark);
        multipleAggExample(spark);
        havingExample(spark);
        emp.groupBy("department")
                .avg("salary")
                .filter("avg(salary) > 60000")
                .show();


        spark.stop();
    }
}