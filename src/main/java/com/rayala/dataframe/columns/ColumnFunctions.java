package com.rayala.dataframe.columns;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class ColumnFunctions {
    static void colVsColumn(Dataset<Row> emp) {
        System.out.println("colVsColumn:");
        //both are same
        emp.select(
                col("name"),
                column("department")
        ).show();
    }

    static void exprExample(Dataset<Row> emp) {
        System.out.println("exprExample:");

        emp.select(
                col("name"),
                col("salary"),
                expr("salary + 5000").alias("new_salary")
        ).show();

        emp.select(expr("upper(name)")).show();

    }

    static void litExample(Dataset<Row> emp) {
        System.out.println("litExample:");
        emp.withColumn("country", lit("India"))
                .show();
        //adding boolean values
        emp.withColumn("is_active", lit(true)).show();

    }

    static void stringFunctions(Dataset<Row> emp) {
        System.out.println("String Functions:");

        emp.select(
                col("name"),
                upper(col("name")).alias("name_upper")
        ).show();

        emp.select(
                col("name"),
                substring(col("city"), 1, 2).alias("city_code")
        ).show();

        emp.select(
                concat(col("name"), lit(" - "), col("department")).alias("info"),
                concat(col("name"), lit(" - "), substring(col("city"), 1, 2)).alias("info2")
        ).show();


    }

    static void whenOtherwiseExample(Dataset<Row> emp) {
        System.out.println("whenOtherwiseExample:");
        emp.withColumn(
                "salary_band",
                when(col("salary").geq(65000), "HIGH")
                        .when(col("salary").geq(55000), "MEDIUM")
                        .otherwise("LOW")
        ).show();
    }

    static void arithmeticExample(Dataset<Row> emp) {
        System.out.println("arithmeticExample:");
        emp.withColumn("double_salary", col("salary").multiply(2))
                .show();
        emp.withColumn("double_salary", expr("salary * 2")).show();

    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Column functions")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> emp = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/employees.csv");

        System.out.println("Dataset emp:");
        emp.show();

        colVsColumn(emp);
        exprExample(emp);
        litExample(emp);
        stringFunctions(emp);
        whenOtherwiseExample(emp);
        arithmeticExample(emp);

        spark.stop();
    }
}
