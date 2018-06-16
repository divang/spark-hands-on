package com.training.spark.examples.dataset;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class RDDToDatasetSchemaProgrammatic {

    public static void main(String[] args) {

	SparkSession session = SparkSession.builder().appName("RDD-to-Dataset-Schema-Programmatic").master("local")
		.getOrCreate();

	JavaRDD<String> stocksRDD = session.read().textFile("src/main/resources/stocks.txt").javaRDD();

	// Generate Schema based on text file data
	List<StructField> fields = new ArrayList<>();
	StructField companySF = DataTypes.createStructField("company", DataTypes.StringType, true);
	fields.add(companySF);
	StructField countStocksSF = DataTypes.createStructField("count", DataTypes.IntegerType, true);
	fields.add(countStocksSF);
	StructField priceSF = DataTypes.createStructField("price", DataTypes.FloatType, true);
	fields.add(priceSF);

	StructType schema = DataTypes.createStructType(fields);

	// Convert text line input to Row
	JavaRDD<Row> rowRDD = stocksRDD.map(new Function<String, Row>() {

	    @Override
	    public Row call(String v1) throws Exception {
		String[] rawStockData = v1.split(",");
		return RowFactory.create(rawStockData[0], Integer.parseInt(rawStockData[1]),
			Float.parseFloat(rawStockData[2]));
	    }
	});

	// Generate Dataset
	Dataset<Row> stockDataFrame = session.createDataFrame(rowRDD, schema);
	stockDataFrame.createOrReplaceTempView("stocks");

	Dataset<Row> results = session.sql("select * from stocks");
	results.show();

	results = session.sql("select max(price) from stocks");
	results.show();

	results = session.sql("select sum(count) from stocks");
	results.show();
    }
}
