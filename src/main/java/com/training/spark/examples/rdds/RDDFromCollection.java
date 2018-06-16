package com.training.spark.examples.rdds;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

// Custom class should implement Serializable, otherwise Spark framework throws
// -> org.apache.spark.SparkException: Task not serializable
public class RDDFromCollection implements Serializable {

    private static final long serialVersionUID = 1517580527817604812L;
    private List<Integer> stockValues = Arrays.asList(100, 200, 300, 400, 500);
    private SparkConf conf;
    private JavaSparkContext context;

    public void init() {
	// The appName parameter is a name for your application to show on the cluster
	// UI.
	String appName = "RDD-From-Collection";
	// master is a Spark, Mesos or YARN cluster URL, or a special “local” string to
	// run in local mode.
	String master = "local";
	conf = new SparkConf().setAppName(appName).setMaster(master);
	// create a JavaSparkContext object, which tells Spark how to access a cluster.
	context = new JavaSparkContext(conf);
    }

    public JavaRDD<Integer> getRDD() {
	// elements of the collection are copied to form a distributed dataset that can
	// be operated on in parallel.
	return context.parallelize(stockValues);
    }

    public JavaRDD<Integer> getRDD(int numOfPartition) {
	// elements of the collection are copied to form a distributed dataset that can
	// be operated on in parallel.
	return context.parallelize(stockValues, numOfPartition);
    }

    public void close() {
	// Closing the RDD
	context.close();
    }

    @Override
    protected void finalize() throws Throwable {
	context.close();
	super.finalize();
    }

    public static void main(String[] args) {
	// Driver program
	RDDFromCollection rddFromCollection = new RDDFromCollection();
	rddFromCollection.init();

	// Default partition
	JavaRDD<Integer> javaRDD = rddFromCollection.getRDD();
	System.out.println("Total count: " + javaRDD.count());
	System.out.println("Total partitions: " + javaRDD.getNumPartitions());

	// Set partition manually
	javaRDD = rddFromCollection.getRDD(3);
	System.out.println("Total count: " + javaRDD.count());
	System.out.println("Total partitions (manually): " + javaRDD.getNumPartitions());

	System.out.println("First in list: " + javaRDD.first());
	System.out.println("Max in list: " + javaRDD.max(new StockComparator()));
	System.out.println("Min in list: " + javaRDD.min(new StockComparator()));

	System.out.println("All Stocks:");
	List<Integer> allStocks = javaRDD.collect();
	// Java 8 Stream foreach API
	allStocks.stream().forEach(new PrintListConsumer());
	System.out.println("All Stocks:");
	// Java 8 double colon operator
	allStocks.stream().forEach(System.out::print);

	rddFromCollection.close();
    }

    // Custom class should implement Serializable, otherwise Spark framework throws
    // -> org.apache.spark.SparkException: Task not serializable
    static class StockComparator implements Comparator<Integer>, Serializable {

	private static final long serialVersionUID = 2717958400841649344L;

	public int compare(Integer o1, Integer o2) {
	    return o1.compareTo(o2);
	}
    }

    static class PrintListConsumer implements Consumer<Integer> {
	public void accept(Integer t) {
	    System.out.print(t + " ");
	}
    }
}
