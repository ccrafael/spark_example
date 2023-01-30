package org.example;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.example.mapper.OutputMapper;
import org.example.model.Input;
import org.example.model.Output;
import scala.Serializable;
import scala.Tuple2;


@AllArgsConstructor
public class LookForPreviousValues implements Serializable {
    private final int value;

    public JavaRDD<Output> lookForPreviousValues(JavaRDD<Input> input) {
        JavaPairRDD<String, Input> countryKeyInput = input.keyBy(Input::getCompany);
        JavaPairRDD<String, Input> countryKeyInputFiltered = input.filter(i -> i.getValue() > this.value)
                .keyBy(Input::getCompany);

        Function2<Tuple2<Input, Input>, Tuple2<Input, Input>, Tuple2<Input, Input>> reduce = (a, b) -> {
            if (a._1.getNumber() > a._2.getNumber() && b._1.getNumber() > b._2.getNumber()) {
                if (a._2.getNumber() < b._2.getNumber()) {
                    return b;
                } else {
                    return a;
                }
            }
            if (a._1.getNumber() > a._2.getNumber() && b._1.getNumber() < b._2.getNumber()) {
                return b;
            }
            if (a._1.getNumber() < a._2.getNumber() && b._1.getNumber() > b._2.getNumber()) {
                return a;
            }
            return a;
        };

        return countryKeyInput.join(countryKeyInputFiltered)
                .values()
                .mapToPair(t -> Tuple2.apply(t._1.getNumber(), t))
                .reduceByKey(reduce)
                .values()
                .map(new OutputMapper());
    }

}
