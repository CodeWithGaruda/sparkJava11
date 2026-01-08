package com.rayala.dataframe.chapter4;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class BasicOperations {
    static void selectExample(Dataset<Row> emp) {
        System.out.println("Select name and salary:");

        emp.select("name", "salary").show();
        //emp.select(col("name"), col("department")).show(); also valid
    }

    static void withColumnExample(Dataset<Row> emp) {
        System.out.println("Add salary_hike column (10%):");

        emp.withColumn("salary_hike", col("salary").multiply(1.10)).show();

        //rename with column
        emp.withColumn("ctc", col("salary")).drop("salary").show();

    }

    static void filterWhereExample(Dataset<Row> emp) {

        System.out.println("Employees salary > 60000:");

        emp.filter(col("salary").gt(60000)).show();

        //sql style
        emp.where("department = 'IT'").show();

        //handle null safety
        emp.filter(col("salary").isNull()).show();


    }

    static void orderByExample(Dataset<Row> emp) {
        System.out.println("Order by salary desc:");

        emp.orderBy(col("salary").desc()).show();

        //multiple sort keys
        emp.orderBy(col("city"), col("salary").desc()).show();

    }

    static void groupByExample(Dataset<Row> emp) {
        System.out.println("Group by salary:");
        RelationalGroupedDataset department = emp.groupBy("department");
        /*
        ✅ Correct mental model

        groupBy() = just promise to group later

        avg(), sum(), count() = execute grouping

        df() = original dataset, not grouped output

        🧪 If you want to “see groups”, do this

            If you want grouped buckets, simulate with collect_list:

            emp.groupBy("department").agg(collect_list("name")).show(false);

            Output:

            +-----------+-----------------------------+
            |department |collect_list(name)           |
            +-----------+-----------------------------+
            |IT         |[Amit, Sunny, Hunter]        |
            |HR         |[Priya, Neha]                |
            |Finance    |[Rahul]                      |
            |null       |[Eagle]                      |
            +-----------+-----------------------------+


            Now you can visually see groups.

            🎯 Key takeaway

            groupBy defines how rows will group.
            Aggregation defines what you do with each group.
            df() just returns the underlying DataFrame, not grouped data.

         */

        department.df().show();

        department.avg("salary").show();

        department.count().show();

    }

    static void aggExample(Dataset<Row> emp) {
        System.out.println("Aggregate salary:");
        emp.groupBy("department")
                .agg(
                        avg("salary").alias("avg_salary"),
                        max("salary").alias("max_salary"),
                        min("salary").alias("min_salary"),
                        count("salary").alias("count_salary")
                    ).show();
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("BasicOperations").master("local[*]").getOrCreate();

        Dataset<Row> emp = spark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/employees.csv");

        emp.show();

        selectExample(emp);
        withColumnExample(emp);
        filterWhereExample(emp);
        orderByExample(emp);
        groupByExample(emp);
        aggExample(emp);

        spark.stop();
    }
}

/*
🔥 Mini-exercise (if you want to try)

Find top 3 highest paid IT employees

Replace null salary with 0 then sort

Find department with max avg salary

Add column salary_level

65000 = HIGH

55000 = MEDIUM

else LOW
 */