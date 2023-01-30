package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.example.model.Input;
import org.example.model.Output;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LookForPreviousValuesTest {

    @Test
    void given_inputs_for_a_country_when_lookForPreviousValues_create_proper_output() {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[8]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Input> inputs = sc.parallelize(Fixture.in);
        LookForPreviousValues transform = new LookForPreviousValues(1000);
        List<Output> out = transform.lookForPreviousValues(inputs).collect();
        assertEquals(out, Fixture.expected);
    }


    public interface Fixture {
        List<Input> in = Arrays.asList(
                Input.builder()
                        .number(1)
                        .company("foo")
                        .value(10)
                        .filePartition(1)
                        .build(),
                Input.builder()
                        .number(2)
                        .company("foo")
                        .value(2000)
                        .filePartition(1)
                        .build(),
                Input.builder()
                        .number(3)
                        .company("foo")
                        .value(2500)
                        .filePartition(1)
                        .build(),
                Input.builder()
                        .number(4)
                        .company("foo")
                        .value(100)
                        .filePartition(1)
                        .build());

        List<Output> expected = Arrays.asList(Output.builder()
                        .partitionFile(1)
                        .number(1)
                        .company("foo")
                        .value(10)
                        .prevNumber(0)
                        .prevValue(0)
                        .build(),
                Output.builder()
                        .partitionFile(1)
                        .number(2)
                        .company("foo")
                        .value(2000)
                        .prevNumber(0)
                        .prevValue(0)
                        .build()
                , Output.builder()
                        .partitionFile(1)
                        .number(3)
                        .company("foo")
                        .value(2500)
                        .prevNumber(2)
                        .prevValue(2000)
                        .build()
                , Output.builder()
                        .partitionFile(1)
                        .number(4)
                        .company("foo")
                        .value(100)
                        .prevNumber(3)
                        .prevValue(2500)
                        .build());
    }
}