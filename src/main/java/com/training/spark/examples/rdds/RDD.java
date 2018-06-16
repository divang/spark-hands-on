package com.training.spark.examples.rdds;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDD {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local")
				.setAppName("First-Spark-Hands-On");

		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> javaRDD = context
				.textFile("/Users/administrator/words.txt");

		System.out.println(javaRDD.count());
		context.close();
	}

}
