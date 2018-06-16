package com.training.spark.examples.rdds;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Consumer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public class RDDFromExternalStorage implements Serializable {
    private static final long serialVersionUID = -6194105340832773675L;
    private SparkConf conf;
    private JavaSparkContext context;

    public void init() {
	conf = new SparkConf().setAppName("RDD-From-External-Storage").setMaster("local");
	context = new JavaSparkContext(conf);
    }

    public void close() {
	context.close();
    }

    public JavaRDD<String> getRDD() {
	// URI for the file (a local path on the machine, and reads it as a collection
	// of lines.
	return context.textFile("src/main/resources/WordCount.txt");
    }

    public void mapAPI(JavaRDD<String> linesRDD) {
	System.out.println("All Words via map API:");
	JavaRDD<String[]> wordsInLinesRDD = linesRDD.map(new MapFun());
	// Print all
	wordsInLinesRDD.collect().forEach(new Consumer<String[]>() {
	    @Override
	    public void accept(String[] t) {
		for (String v : t) {
		    System.out.println(v);
		}
	    }
	});
    }

    public void flatMapAPI(JavaRDD<String> linesRDD) {
	System.out.println("All Words via flatMap API:");
	JavaRDD<String> allWords = linesRDD.flatMap(new FlatMapFun());
	// Print all
	allWords.collect().forEach(System.out::println);
    }

    public static void main(String[] args) {
	RDDFromExternalStorage externalStorage = new RDDFromExternalStorage();
	externalStorage.init();
	JavaRDD<String> linesRDD = externalStorage.getRDD();
	System.out.println("No of lines in file: " + linesRDD.count());

	externalStorage.mapAPI(linesRDD);
	externalStorage.flatMapAPI(linesRDD);
	externalStorage.close();
    }

    static class MapFun implements Function<String, String[]>, Serializable {

	@Override
	public String[] call(String v1) throws Exception {
	    return v1.split(" ");
	}
    }

    static class FlatMapFun implements FlatMapFunction<String, String>, Serializable {

	@Override
	public Iterator<String> call(String t) throws Exception {
	    return Arrays.asList(t.split(" ")).iterator();
	}

    }
}
