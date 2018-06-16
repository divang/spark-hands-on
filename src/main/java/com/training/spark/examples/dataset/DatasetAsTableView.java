package com.training.spark.examples.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DatasetAsTableView {

    public static void main(String[] args) {
	SparkSession sparkSession = SparkSession.builder().appName("Dataset-Table-View").master("local").getOrCreate();

	Dataset<Row> df = sparkSession.read().json("src/main/resources/people.json");
	/*
	 * Temporary views in Spark SQL are session-scoped and will disappear if the
	 * session that creates it terminates.
	 */
	df.createOrReplaceTempView("people");

	Dataset<Row> sqlDF = sparkSession.sql("select * from people");
	sqlDF.show();

	/*
	 * want to have a temporary view that is shared among all sessions and keep
	 * alive until the Spark application terminates, you can create a global
	 * temporary view. Global temporary view is tied to a system preserved database
	 * global_temp, and we must use the qualified name to refer it, e.g. SELECT *
	 * FROM global_temp.view1.
	 */
	df.createOrReplaceGlobalTempView("people");
	sparkSession.sql("select * from global_temp.people").show();
	sparkSession.newSession().sql("select * from global_temp.people").show();
    }
}
