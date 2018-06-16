package com.training.spark.examples.userdefine.function;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class StockAverage extends UserDefinedAggregateFunction {

    private static final long serialVersionUID = 6050423082448286247L;
    private StructType inputSchema;
    private StructType bufferSchema;

    // Data types of values in the aggregation buffer
    @Override
    public StructType bufferSchema() {
	return bufferSchema;
    }

    // The data type of the returned value
    @Override
    public DataType dataType() {
	return DataTypes.DoubleType;
    }

    // Whether this function always returns the same output on the identical input
    @Override
    public boolean deterministic() {
	return true;
    }

    @Override
    public Object evaluate(Row arg0) {
	return null;
    }

    @Override
    public void initialize(MutableAggregationBuffer arg0) {

    }

    // Data types of input arguments of this aggregate function
    @Override
    public StructType inputSchema() {
	return inputSchema;
    }

    @Override
    public void merge(MutableAggregationBuffer arg0, Row arg1) {

    }

    @Override
    public void update(MutableAggregationBuffer arg0, Row arg1) {

    }

}
