package com.training.spark.examples.rdds.operations;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class RddMapPartition {

    public static void main(String[] args) {
	List<Integer> collectionData = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	SparkConf sparkConf = new SparkConf().setAppName("Map-Partitioner-Operation").setMaster("local");
	JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
	JavaRDD<Integer> javaRDDPar = javaSparkContext.parallelize(collectionData, 2);

	javaRDDPar.map(new Function<Integer, Integer>() {

	    @Override
	    public Integer call(Integer v1) throws Exception {
		System.out.println("Map API --->" + v1);
		return v1;
	    }
	}).collect();

	/*
	 * Similar to map, but runs separately on each partition (block) of the RDD, so
	 * func must be of type Iterator<T> => Iterator<U> when running on an RDD of
	 * type T.
	 */
	System.out.println("No of partitiioner: " + javaRDDPar.getNumPartitions());
	javaRDDPar.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {

	    @Override
	    public Iterator<Integer> call(Iterator<Integer> t) throws Exception {

		System.out.println("Map Partition API--->");
		Iterable<Integer> iterable = new Iterable<Integer>() {
		    @Override
		    public Iterator<Integer> iterator() {
			return t;
		    }
		};
		StreamSupport.stream(iterable.spliterator(), false).forEach(new Consumer<Integer>() {

		    @Override
		    public void accept(Integer t) {
			System.out.println(" " + t);
		    }
		});

		return t;
	    }
	}).collect();

	javaRDDPar.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {

	    @Override
	    public Iterator<Integer> call(Integer v1, Iterator<Integer> v2) throws Exception {
		System.out.println("Partition Index:" + v1);
		Iterable<Integer> it = () -> v2;
		StreamSupport.stream(it.spliterator(), false).forEach(t -> System.out.println(" " + t));
		return v2;
	    }
	}, true).collect();

	javaRDDPar.mapPartitionsWithIndex((v1, v2) -> {
	    System.out.println("part: " + v1);
	    Iterable<Integer> it = () -> v2;
	    StreamSupport.stream(it.spliterator(), false).forEach(t -> System.out.println(" " + t));
	    return v2;
	}, true).collect();

	javaSparkContext.close();
    }
}
