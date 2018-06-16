package com.training.spark.examples.dataset.bean;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DatasetBean {

    public static void main(String[] args) {
	SparkSession session = SparkSession.builder().appName("First-Bean").master("local").getOrCreate();

	// Bean class instance
	Person person1 = new Person();
	person1.setName("James");
	person1.setAge(35);

	Person person2 = new Person();
	person2.setName("Ally");
	person2.setAge(30);

	List<Person> persons = new ArrayList<>();
	persons.add(person1);
	persons.add(person2);
	/*
	 * Datasets are similar to RDDs, however, instead of using Java serialization or
	 * Kryo they use a specialized Encoder to serialize the objects for processing
	 * or transmitting over the network. While both encoders and standard
	 * serialization are responsible for turning an object into bytes, encoders are
	 * code generated dynamically and use a format that allows Spark to perform many
	 * operations like filtering, sorting and hashing without deserializing the
	 * bytes back into an object.
	 */
	Encoder<Person> encoderPerson = Encoders.bean(Person.class);
	Dataset<Row> dataset = session.createDataFrame(persons, Person.class);
	dataset.show();

	// Read from Json file and map the data with Bean class
	Dataset<Person> personDS = session.read().json("src/main/resources/people.json").as(encoderPerson);
	personDS.show();
    }
}
