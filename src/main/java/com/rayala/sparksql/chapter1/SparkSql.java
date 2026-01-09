package com.rayala.sparksql.chapter1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SparkSql {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkFirstProgram")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> emp = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/emp.csv");

        emp.createOrReplaceTempView("employees");

        spark.sql("SELECT name, salary FROM employees WHERE salary > 60000").show();

        System.out.println("Explain in spark sql");
        spark.sql("SELECT * FROM employees WHERE salary > 60000")
                .explain(true);
        System.out.println("Explain in Dataframe");
        emp.filter(col("salary").gt(60000)).explain(true);
    }
}
