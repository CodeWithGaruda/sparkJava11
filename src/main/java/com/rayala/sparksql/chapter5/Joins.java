package com.rayala.sparksql.chapter5;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Joins {
    static void innerJoinExample(SparkSession spark) {
        System.out.println("---- INNER JOIN ----");

        spark.sql(
                "SELECT e.name, d.dept_name, e.salary " +
                        "FROM employees e " +
                        "INNER JOIN departments d " +
                        "ON e.dept_id = d.dept_id"
        ).show();
    }

    static void leftJoinExample(SparkSession spark) {
        System.out.println("---- LEFT JOIN ----");

        spark.sql(
                "SELECT e.name, d.dept_name, e.salary " +
                        "FROM employees e " +
                        "LEFT JOIN departments d " +
                        "ON e.dept_id = d.dept_id"
        ).show();
    }

    static void rightJoinExample(SparkSession spark) {
        System.out.println("---- RIGHT JOIN ----");

        spark.sql(
                "SELECT e.name, d.dept_name, e.salary " +
                        "FROM employees e " +
                        "RIGHT JOIN departments d " +
                        "ON e.dept_id = d.dept_id"
        ).show();
    }

    static void fullJoinExample(SparkSession spark) {
        System.out.println("---- FULL JOIN ----");

        spark.sql(
                "SELECT e.name, d.dept_name, e.salary " +
                        "FROM employees e " +
                        "FULL OUTER JOIN departments d " +
                        "ON e.dept_id = d.dept_id"
        ).show();
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("SparkSqlJoins")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> emp = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/emp.csv");

        Dataset<Row> dept = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/dep.csv");

        emp.createOrReplaceTempView("employees");
        dept.createOrReplaceTempView("departments");

        innerJoinExample(spark);
        leftJoinExample(spark);
        rightJoinExample(spark);
        fullJoinExample(spark);

        spark.stop();
    }
}