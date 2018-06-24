package com.training.spark.examples.streaming;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

public class WordCountFromKafka {

    public static void main(String[] args) {

	SparkSession session = SparkSession.builder().appName("Word-Count-Kafka").master("local[2]").getOrCreate();
	JavaSparkContext javaSparkContext = new JavaSparkContext(session.sparkContext());
	JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext,
		Durations.milliseconds(5 * 1000));

	Map<String, String> kafkaParam = new HashMap<>();
	kafkaParam.put("metadata.broker.list", "localhost:9092");

	JavaPairInputDStream<String, String> kafkaDStream = KafkaUtils.createDirectStream(javaStreamingContext,
		String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParam,
		Collections.singleton("test"));

	kafkaDStream.print();

	javaStreamingContext.start();
	try {
	    javaStreamingContext.awaitTermination();
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}
    }
}
