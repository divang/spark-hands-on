package com.training.spark.examples.rdds.operations;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Consumer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class RddOperations {
    private static SparkSession session = SparkSession.builder().appName("RDD-Operations").master("local")
	    .getOrCreate();
    private static JavaRDD<String> javaRDD = session.read().textFile("src/main/resources/WordCount.txt").javaRDD();

    public static void totalWordCountInTextFile() {
	Integer totalWord = javaRDD.map(new Function<String, Integer>() {

	    @Override
	    public Integer call(String v1) throws Exception {

		return v1.split(" ").length;
	    }
	}).reduce(new Function2<Integer, Integer, Integer>() {

	    @Override
	    public Integer call(Integer v1, Integer v2) throws Exception {
		// TODO Auto-generated method stub
		return v1 + v2;
	    }
	});
	System.out.println("Total word: " + totalWord);
    }

    public static void wordWiseCount() {

	javaRDD.flatMap(new FlatMapFunction<String, String>() {
	    @Override
	    public Iterator<String> call(String t) throws Exception {
		return Arrays.asList(t.split(" ")).iterator();
	    }
	}).mapToPair(new PairFunction<String, String, Integer>() {

	    @Override
	    public Tuple2<String, Integer> call(String t) throws Exception {

		return new Tuple2<String, Integer>(t, 1);
	    }
	}).reduceByKey(new Function2<Integer, Integer, Integer>() {

	    @Override
	    public Integer call(Integer v1, Integer v2) throws Exception {
		return v1 + v2;
	    }
	}).collect().forEach(new Consumer<Tuple2<String, Integer>>() {

	    @Override
	    public void accept(Tuple2<String, Integer> t) {
		System.out.println("word: " + t._1 + " count: " + t._2);
	    }
	});
    }

    public static void main(String[] args) {
	wordWiseCount();
    }
}
