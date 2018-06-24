package com.training.spark.examples.streaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WordCountFromTCPConnection {

    public static void main(String[] args) {

	SparkSession session = SparkSession.builder().appName("Word-Count-From-TCP-Connection").master("local[2]")
		.getOrCreate();
	JavaSparkContext javaSparkContext = new JavaSparkContext(session.sparkContext());
	JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, Durations.seconds(2));
	JavaReceiverInputDStream<String> lineFromTCPConnection = javaStreamingContext.socketTextStream("localhost",
		9999);

	JavaDStream<String> tcpConnectionWords = lineFromTCPConnection.flatMap(new FlatMapFunction<String, String>() {
	    @Override
	    public Iterator<String> call(String t) throws Exception {

		return Arrays.asList(t.split(" ")).iterator();
	    }
	});

	JavaPairDStream<String, Integer> wordCountPair = tcpConnectionWords
		.mapToPair(new PairFunction<String, String, Integer>() {

		    @Override
		    public Tuple2<String, Integer> call(String t) throws Exception {
			return new Tuple2<String, Integer>(t, 1);
		    }
		});

	JavaPairDStream<String, Integer> wordCount = wordCountPair
		.reduceByKey(new Function2<Integer, Integer, Integer>() {

		    @Override
		    public Integer call(Integer v1, Integer v2) throws Exception {

			return v1 + v2;
		    }
		});

	wordCount.print();

	javaStreamingContext.start();
	try {
	    javaStreamingContext.awaitTerminationOrTimeout(10 * 60 * 1000);
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}
    }
}
