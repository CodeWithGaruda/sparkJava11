package com.rayala.sparksql.chapter6;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WindowFunctions {
    static void rowNumberExample(SparkSession spark) {
        System.out.println("---- row_number ----");

        spark.sql(
                "SELECT name, department, salary, " +
                        "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn " +
                        "FROM employees"
        ).show();
    }

    static void rankExample(SparkSession spark) {
        System.out.println("---- rank ----");

        spark.sql(
                "SELECT name, department, salary, " +
                        "RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk " +
                        "FROM employees"
        ).show();
    }

    static void denseRankExample(SparkSession spark) {
        System.out.println("---- dense_rank ----");

        spark.sql(
                "SELECT name, department, salary, " +
                        "DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS drnk " +
                        "FROM employees"
        ).show();
    }

    static void windowAggregationExample(SparkSession spark) {
        System.out.println("---- Window Aggregation ----");

        spark.sql(
                "SELECT name, department, salary, " +
                        "AVG(salary) OVER (PARTITION BY department) AS avg_dept_salary " +
                        "FROM employees"
        ).show();
    }


    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("SparkSqlWindowFunctions")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> emp = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/employees.csv");

        emp.createOrReplaceTempView("employees");

        rowNumberExample(spark);
        rankExample(spark);
        denseRankExample(spark);
        windowAggregationExample(spark);

        spark.stop();
    }
}
