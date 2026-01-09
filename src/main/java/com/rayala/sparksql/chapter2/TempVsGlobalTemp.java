package com.rayala.sparksql.chapter2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TempVsGlobalTemp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkFirstProgram")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> emp = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/emp.csv");
        createTempView(spark, emp);
        createGlobalTempTable(spark, emp);
    }

    static void createTempView(SparkSession spark, Dataset<Row> emp) {
        //creation of Temporary view
        emp.createOrReplaceTempView("employees");
        //how to use
        spark.sql("SELECT dept_id, AVG(salary) AS avg_salary FROM employees GROUP BY dept_id").show();
    }

    static void createGlobalTempTable(SparkSession spark, Dataset<Row> emp) {
        emp.createOrReplaceGlobalTempView("employees");
        spark.sql("SELECT * FROM global_temp.employees ").show();
    }
}
