package com.rayala;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.*;

import static org.apache.spark.sql.functions.*;

public class Main {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("PracticeExamples")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        System.out.println("===== RDD EXAMPLES =====");
        rdd1(sc);
        rdd2(sc);
        rdd3(sc);

        System.out.println("===== DATAFRAME EXAMPLES =====");
        df1(spark);
        df2(spark);
        df3(spark);

        System.out.println("===== SPARK SQL EXAMPLES =====");
        sqlExamples(spark);

        spark.stop();
    }

    // ---------------- RDD Examples ----------------

    static void rdd1(JavaSparkContext sc) {
        JavaRDD<Integer> rdd =
                sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9));

        long evens = rdd.filter(n -> n % 2 == 0).count();
        long odds  = rdd.filter(n -> n % 2 != 0).count();

        System.out.println("RDD1 -> Even = " + evens + ", Odd = " + odds);
    }

    static void rdd2(JavaSparkContext sc) {
        JavaRDD<String> rdd =
                sc.parallelize(Arrays.asList(
                        "hello spark hello bigdata spark world".split(" ")
                ));

        Map<String, Long> result = rdd.countByValue();

        System.out.println("RDD2 -> Word Count = " + result);
    }

    static void rdd3(JavaSparkContext sc) {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(10, 4, 20, 7, 30));

        int max = rdd.reduce(Integer::max);
        int min = rdd.reduce(Integer::min);
        int sum = rdd.reduce(Integer::sum);
        double avg = sum / (double) rdd.count();

        System.out.println("RDD3 -> max=" + max + ", min=" + min + ", avg=" + avg);
    }

    // ---------------- DataFrame Examples ----------------

    static void df1(SparkSession spark) {

        List<Row> data = Arrays.asList(
                RowFactory.create("Amit", 23),
                RowFactory.create("Sunny", 27),
                RowFactory.create("Priya", 30)
        );

        StructType schema = new StructType()
                .add("name", "string")
                .add("age", "integer");

        Dataset<Row> df = spark.createDataFrame(data, schema);

        System.out.println("DF1 -> Age > 25");
        df.filter(col("age").gt(25)).show();
    }

    static void df2(SparkSession spark) {

        Dataset<Row> people = spark.read()
                .option("multiline", true)
                .json("src/main/resources/people.json");

        System.out.println("DF2 -> Group by City Count");
        people.groupBy("city").count().show();
    }

    static void df3(SparkSession spark) {

        Dataset<Row> emp = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/employee.csv");

        System.out.println("DF3 -> Average Salary by Department");
        emp.groupBy("department")
                .avg("salary")
                .show();
    }

    // ---------------- SQL Examples ----------------

    static void sqlExamples(SparkSession spark) {

        Dataset<Row> emp = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/employee.csv");

        Dataset<Row> dept = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/departments.csv");

        emp.createOrReplaceTempView("employees");
        dept.createOrReplaceTempView("departments");

        System.out.println("SQL1 -> Avg salary per department");

        String s1="SELECT department, AVG(salary) AS avg_salary FROM employees GROUP BY department";
        spark.sql(s1).show();
String s2=" SELECT name, salary\n" +
        "                FROM employees\n" +
        "                ORDER BY salary DESC\n" +
        "                LIMIT 3";
        System.out.println("SQL2 -> Top 3 salaries");
        spark.sql(s2).show();

        System.out.println("SQL3 -> Join employees + departments");
        String s3="SELECT e.name, d.dept_name\n" +
                "                FROM employees e\n" +
                "                JOIN departments d\n" +
                "                ON e.department = d.id";
        spark.sql(s3).show();
    }
}
