package com.rayala.dataframe.chatper8;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class AdvancedAggregations {

    static void groupByRecap(Dataset<Row> emp) {
        System.out.println("groupByRecap");
        emp.groupBy("dept_id")
                .avg("salary")
                .show();
    }

    static void rollupExample(Dataset<Row> emp) {
        System.out.println("rollupExample:Think subtotal + grand total.");
        System.out.println("---- ROLLUP ----");
        emp.rollup("dept_id")
                .agg(avg("salary").alias("avg_salary"))
                .show();
    }

    static void cubeExample(Dataset<Row> emp) {

        System.out.println("---- CUBE ----");
        emp.cube("dept_id", "city")
                .agg(avg("salary").alias("avg_salary"))
                .show();
        /*
        This generates:
            by dept
            by city
            by dept + city
            overall total
         */
    }

    static void windowFunctions(Dataset<Row> emp) {
        System.out.println("WindowFunctions");
        WindowSpec w = Window
                .partitionBy("dept_id")
                .orderBy(col("salary").desc());
        //assign unique row number per partition
        emp.withColumn("row_number", row_number().over(w)).show();
        //rank vs dense rank
        emp.withColumn("rank", rank().over(w))
                .withColumn("dense_rank", dense_rank().over(w))
                .show();
        /*
        Difference:
        | Function   | Gaps? | Example    |
        | ---------- | ----- | ---------- |
        | rank       | YES   | 1, 2, 2, 4 |
        | dense_rank | NO    | 1, 2, 2, 3 |

        🔥 Real use case

        “Top 3 highest paid employees per department”

        emp.withColumn("rank", dense_rank().over(w))
           .filter(col("rank").leq(3))
           .show();
         */

    }

    static void movingAverage(Dataset<Row> emp) {
        System.out.println("movingAverage");
        WindowSpec w = Window
                .partitionBy("dept_id")
                .orderBy("emp_id")
                .rowsBetween(-2, 0);

        /*
        Meaning:
            current row
            previous 2 rows
            average salary
         */
        emp.withColumn("moving_avg_salary", avg("salary").over(w)).show();
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Advanced Aggregations")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> emp = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/emp.csv");

        emp.show();

        groupByRecap(emp);
        rollupExample(emp);
        cubeExample(emp);
        windowFunctions(emp);
        movingAverage(emp);

        spark.stop();
    }
}
