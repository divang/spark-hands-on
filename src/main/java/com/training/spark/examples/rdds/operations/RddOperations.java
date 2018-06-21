package com.training.spark.examples.rdds.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

    public static void sampleAPI() {
	List<Integer> collectionData = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	JavaSparkContext.fromSparkContext(session.sparkContext()).parallelize(collectionData).sample(true, .4).collect()
		.forEach(new Consumer<Integer>() {

		    @Override
		    public void accept(Integer t) {
			System.out.println(" " + t);

		    }
		});
    }

    public static void unionAPI() {
	List<Integer> collectionDataFirst = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
	JavaRDD<Integer> javaRDD = JavaSparkContext.fromSparkContext(session.sparkContext())
		.parallelize(collectionDataFirst);
	List<Integer> collectionDataSecond = Arrays.asList(4, 5, 6, 7, 8, 9, 10);
	JavaRDD<Integer> javaRDD2 = JavaSparkContext.fromSparkContext(session.sparkContext())
		.parallelize(collectionDataSecond);

	JavaRDD<Integer> unioRDD = javaRDD.union(javaRDD2);
	unioRDD.collect().forEach(t -> System.out.println(t));
    }

    public static void intersectionAPI() {
	List<Integer> collectionDataFirst = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
	JavaRDD<Integer> javaRDD = JavaSparkContext.fromSparkContext(session.sparkContext())
		.parallelize(collectionDataFirst);
	List<Integer> collectionDataSecond = Arrays.asList(4, 5, 6, 7, 8, 9, 10);
	JavaRDD<Integer> javaRDD2 = JavaSparkContext.fromSparkContext(session.sparkContext())
		.parallelize(collectionDataSecond);

	JavaRDD<Integer> intersectionRDD = javaRDD.intersection(javaRDD2);
	intersectionRDD.collect().forEach(t -> System.out.println(t));
    }

    public static void distinctAPI() {
	List<Integer> collectionDataFirst = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 7, 7, 6, 6, 8, 9, 10);
	JavaRDD<Integer> javaRDD = JavaSparkContext.fromSparkContext(session.sparkContext())
		.parallelize(collectionDataFirst);

	JavaRDD<Integer> distinctRDD = javaRDD.distinct();
	distinctRDD.collect().forEach(t -> System.out.println(t));
    }

    /*
     * When called on a dataset of (K, V) pairs, returns a dataset of (K,
     * Iterable<V>) pairs. Note: If you are grouping in order to perform an
     * aggregation (such as a sum or average) over each key, using reduceByKey or
     * aggregateByKey will yield much better performance. Note: By default, the
     * level of parallelism in the output depends on the number of partitions of the
     * parent RDD. You can pass an optional numPartitions argument to set a
     * different number of tasks.
     * 
     * Drawback: groupByKey can cause out of disk problems as data is sent over the
     * network and collected on the reduce workers.
     */
    // Age wise grouping
    public static void groupByAPI() {
	JavaRDD<String> peopleDS = session.read().textFile("src/main/resources/people.txt").javaRDD();
	peopleDS.mapToPair(new PairFunction<String, Integer, String>() {
	    @Override
	    public Tuple2<Integer, String> call(String t) throws Exception {
		String[] split = t.split(",");
		return new Tuple2<Integer, String>(Integer.parseInt(split[1].trim()), split[0]);
	    }
	}).groupByKey().collect().forEach(k -> System.out.println(k));
    }

    /*
     * When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs
     * where the values for each key are aggregated using the given reduce function
     * func, which must be of type (V,V) => V. Like in groupByKey, the number of
     * reduce tasks is configurable through an optional second argument.
     * 
     * Advantage: Data is combined at each partition , only one output for one key
     * at each partition to send over network. reduceByKey required combining all
     * your values into another value with the "exact same type".
     */
    // Age wise grouping
    public static void reduceByKeyAPI() {
	JavaRDD<String> peopleDS = session.read().textFile("src/main/resources/people.txt").javaRDD();
	peopleDS.mapToPair(row -> {
	    String[] items = row.split(",");
	    return new Tuple2<Integer, String>(Integer.parseInt(items[1].trim()), items[0]);
	}).reduceByKey(new Function2<String, String, String>() {
	    @Override
	    public String call(String v1, String v2) throws Exception {
		System.out.println("v1:" + v1 + " v2:" + v2);
		return v1 + " " + v2;
	    }
	}).collect().forEach(t -> System.out.println(t));
    }

    // same as reduceByKey, which takes an initial value.
    public static void aggregateByKeyAPI() {
	List<String> sameAgePeople = new ArrayList<>();
	JavaRDD<String> peopleDS = session.read().textFile("src/main/resources/people.txt").javaRDD();
	peopleDS.mapToPair(row -> {
	    String[] items = row.split(",");
	    return new Tuple2<Integer, String>(Integer.parseInt(items[1].trim()), items[0]);
	}).aggregateByKey(sameAgePeople, new Function2<List<String>, String, List<String>>() {

	    @Override
	    public List<String> call(List<String> v1, String v2) throws Exception {
		// TODO Auto-generated method stub
		v1.add(v2);
		return v1;
	    }

	}, new Function2<List<String>, List<String>, List<String>>() {

	    @Override
	    public List<String> call(List<String> v1, List<String> v2) throws Exception {
		// TODO Auto-generated method stub
		v1.addAll(v2);
		return v1;
	    }

	}).collect().forEach(t -> System.out.println(t));

    }

    public static void sortByKey() {

	javaRDD.flatMap(new FlatMapFunction<String, String>() {

	    @Override
	    public Iterator<String> call(String t) throws Exception {
		return Arrays.asList(t.split(" ")).iterator();
	    }
	}).sortBy(new Function<String, String>() {

	    @Override
	    public String call(String v1) throws Exception {
		return v1;
	    }
	}, true, 1).collect().forEach(t -> System.out.println(t));

    }

    public static void reduceAPI() {
	String output = javaRDD.flatMap(new FlatMapFunction<String, String>() {

	    @Override
	    public Iterator<String> call(String t) throws Exception {
		return Arrays.asList(t.split(" ")).iterator();
	    }
	}).reduce(new Function2<String, String, String>() {

	    @Override
	    public String call(String v1, String v2) throws Exception {

		return v1 + " " + v2;
	    }
	});
	System.out.println("reduce- " + output);
    }

    public static void main(String[] args) {
	// wordWiseCount();
	// sampleAPI();
	// unionAPI();
	// intersectionAPI();
	// distinctAPI();
	// groupByAPI();
	// reduceByKeyAPI();
	// aggregateByKeyAPI();
	// sortByKey();
	reduceAPI();
    }

}
