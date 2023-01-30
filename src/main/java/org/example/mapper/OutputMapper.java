package org.example.mapper;


import org.apache.spark.api.java.function.Function;
import org.example.model.Input;
import org.example.model.Output;
import scala.Tuple2;


public class OutputMapper implements Function<Tuple2<Input, Input>, Output> {


    @Override
    public Output call(Tuple2<Input, Input> a) {
        return Output.builder()
                .number(a._1.getNumber())
                .value(a._1.getValue())
                .company(a._1.getCompany())
                .prevNumber(a._1.getNumber() > a._2.getNumber() ? a._2.getNumber() : 0)
                .prevValue(a._1.getNumber() > a._2.getNumber() ? a._2.getValue() : 0)
                .partitionFile(a._1.getFilePartition())
                .build();
    }
}
