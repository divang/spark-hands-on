package com.training.spark.examples.dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.training.spark.examples.dataset.bean.Person;

public class RDDToDatasetSchemaInferring {

    public static void main(String[] args) {

	SparkSession session = SparkSession.builder().appName("RDD-to-Dataset-Schema-Inferring").master("local")
		.getOrCreate();

	// Read the raw text file and then map with bean fields
	Dataset<String> dataset = session.read().textFile("src/main/resources/people.txt");
	JavaRDD<Person> personsRDD = dataset.javaRDD().map(new Function<String, Person>() {
	    private static final long serialVersionUID = -8523892256141383269L;

	    // Now map the text file comma separated values are mapped to bean field
	    // Inferring (guessing) the schema
	    @Override
	    public Person call(String v1) throws Exception {
		String[] items = v1.split(",");
		Person person = new Person();
		person.setName(items[0]);
		person.setAge(Integer.parseInt(items[1].trim()));
		return person;
	    }
	});
	// print JavaRDD
	personsRDD.foreach(new VoidFunction<Person>() {

	    private static final long serialVersionUID = 6945696725931125111L;

	    @Override
	    public void call(Person t) throws Exception {
		System.out.println(t);
	    }
	});

	// Convert JavaRDD to DataFrame
	Dataset<Row> personDF = session.createDataFrame(personsRDD, Person.class);

	personDF.createOrReplaceTempView("person");

	Dataset<Row> teenAge = session.sql("select name, age from person where age > 13 and age < 19");
	teenAge.show();

	// To access the Dataset via column index
	Dataset<String> namesViaIndexFieldDS = teenAge.map(new MapFunction<Row, String>() {

	    @Override
	    public String call(Row value) throws Exception {
		// TODO Auto-generated method stub
		return "name:" + value.getString(0) + " age: " + value.getInt(1);
	    }
	}, Encoders.STRING());
	namesViaIndexFieldDS.show();

	// To access the column via column name
	Dataset<String> nameViaNameFieldDs = teenAge.map(new MapFunction<Row, String>() {

	    @Override
	    public String call(Row value) throws Exception {

		return "Age: " + value.getAs("age") + " name: " + value.<String>getAs("name");
	    }
	}, Encoders.STRING());
	nameViaNameFieldDs.show();
    }
}
