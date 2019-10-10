package com.wjy.zh13.sql;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class StringCount extends UserDefinedAggregateFunction {
    @Override
    public StructType inputSchema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("str", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(structFields);
        return structType;
    }

    @Override
    public StructType bufferSchema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("count", DataTypes.IntegerType, true));
        return DataTypes.createStructType(structFields);
    }

    @Override
    public DataType dataType() {
        return DataTypes.IntegerType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0,0D);
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        if(!input.isNullAt(0)){
            double updateSum = buffer.getDouble(0) + input.getDouble(0);
//            double updateCount = buffer.getDouble(1) + 1;
            buffer.update(0,updateSum);
//            buffer.update(1,updateCount);
        }
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

    }

    @Override
    public Object evaluate(Row buffer) {
        return null;
    }
}
