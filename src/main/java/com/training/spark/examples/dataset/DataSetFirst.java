package com.training.spark.examples.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class DataSetFirst {

    public static void main(String[] args) {

	SparkSession spark = SparkSession.builder().appName("DataSet-First").master("local").getOrCreate();

	Dataset<Row> df = spark.read().json("src/main/resources/employees.json");
	// Display the content of the DataFrame to stdout
	df.show();

	// Print the schema in tree format
	df.printSchema();

	// Select the "name" column
	df.select("name").show();

	Dataset<Row> peopleDF = spark.read().json("src/main/resources/people.json");
	peopleDF.show();
	// Select all and increment age by 1
	peopleDF.select(functions.col("name"), functions.col("age").plus("1")).show();
	// Select people older then 20, use filter API
	peopleDF.filter(functions.col("age").gt(20)).show();
	// Group by age
	peopleDF.groupBy(functions.col("age")).count().show();
    }
}
