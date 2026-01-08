package com.rayala.dataframe.chapter2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class ColumnOperations {
    static void basicSelect(Dataset<Row> emp) {
        System.out.println("Select name and salary:");

        emp.select("name", "salary").show();
    }

    static void renameColumns(Dataset<Row> emp) {

        System.out.println("Rename salary to ctc:");

        emp.withColumnRenamed("salary", "ctc").show();
    }

    static void addNewColumn(Dataset<Row> emp) {

        System.out.println("Add new salary_hike column:");

        emp.withColumn("salary_hike", col("salary").multiply(1.10)).show();
    }

    static void conditionalColumn(Dataset<Row> emp) {

        System.out.println("Add grade column:");

        emp.withColumn(
                "grade", when(col("salary").geq(65000), "A")
                                .when(col("salary").geq(60000), "B")
                                .otherwise("C")
        ).show();
    }

    static void stringFunctions(Dataset<Row> emp) {

        System.out.println("Uppercase names and string length:");

            emp.withColumn("upper_name", upper(col("name")))
                .withColumn("name_length", length(col("name")))
                .show();
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("ColumnOperations").master("local[*]").getOrCreate();

        Dataset<Row> emp = spark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/employees.csv");

        emp.show();

        basicSelect(emp);
        renameColumns(emp);
        addNewColumn(emp);
        conditionalColumn(emp);
        stringFunctions(emp);

        spark.stop();
    }
}
