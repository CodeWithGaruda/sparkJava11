package com.rayala.dataframe.chapter7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.broadcast;

public class Joins {
    static void innerJoin(Dataset<Row> emp, Dataset<Row> dept) {

        System.out.println("---- INNER JOIN ----");
        emp.join(dept, emp.col("dept_id").equalTo(dept.col("dept_id")), "inner").show();
    }

    static void leftJoin(Dataset<Row> emp, Dataset<Row> dept) {

        System.out.println("---- LEFT JOIN ----");
        emp.join(dept, emp.col("dept_id").equalTo(dept.col("dept_id")), "left").show();
    }

    static void rightJoin(Dataset<Row> emp, Dataset<Row> dept) {

        System.out.println("---- RIGHT JOIN ----");
        emp.join(dept, emp.col("dept_id").equalTo(dept.col("dept_id")), "right").show();
    }

    static void fullJoin(Dataset<Row> emp, Dataset<Row> dept) {

        System.out.println("---- FULL OUTER JOIN ----");
        emp.join(dept, emp.col("dept_id").equalTo(dept.col("dept_id")), "outer").show();
    }

    static void semiJoin(Dataset<Row> emp, Dataset<Row> dept) {

        System.out.println("---- LEFT SEMI JOIN ----");
        emp.join(dept, emp.col("dept_id").equalTo(dept.col("dept_id")), "left_semi").show();
    }

    static void antiJoin(Dataset<Row> emp, Dataset<Row> dept) {

        System.out.println("---- LEFT ANTI JOIN ----");
        emp.join(dept, emp.col("dept_id").equalTo(dept.col("dept_id")), "left_anti").show();
    }

    private static void joinDifferentColumns(Dataset<Row> emp, Dataset<Row> dept) {
        System.out.println("---- JOIN DIFFERENT COLUMNS ----");
        emp.join(dept, emp.col("dept_id").equalTo(dept.col("dept_code"))).show();

    }

    static void broadcastJoin(Dataset<Row> emp, Dataset<Row> dept) {
        System.out.println("---- BROADCAST JOIN ----");
        emp.join(broadcast(dept), "dept_id").show();
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Join Examples")
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

        emp.show();
        dept.show();

        innerJoin(emp, dept);
        leftJoin(emp, dept);
        rightJoin(emp, dept);
        fullJoin(emp, dept);
        semiJoin(emp, dept);
        antiJoin(emp, dept);
        joinDifferentColumns(emp, dept);
        broadcastJoin(emp, dept);

        spark.stop();
    }
}
/*
🔥 Small exercises (optional)

Try:

Find employees WITHOUT salary but WITH department

Find departments WITHOUT employees

Highest paid employee per department using join + groupBy
 */