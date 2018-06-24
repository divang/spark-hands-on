package com.training.spark.examples.broadcast.var;

import java.util.Arrays;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

public class BroadCastVariables {

    public static void main(String[] args) throws InterruptedException {

	SparkSession session = SparkSession.builder().appName("Broad-Casr-Var").master("local").getOrCreate();
	JavaSparkContext javaSparkContext = new JavaSparkContext(session.sparkContext());

	int[] data = new int[] { 1, 2, 3, 4, 5 };
	Broadcast<int[]> broadcast = javaSparkContext.broadcast(data);

	LongAccumulator accumulator = session.sparkContext().longAccumulator("Total");
	javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5)).foreach(x -> accumulator.add(x));

	System.out.println(accumulator.value());

    }
}
