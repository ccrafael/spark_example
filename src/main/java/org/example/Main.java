package org.example;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.example.mapper.InputMapper;
import org.example.model.Input;
import org.example.model.Output;
import org.jetbrains.annotations.NotNull;

public class Main {
    public static void main(String[] args) {
        try (JavaSparkContext sc = createSparkContext()) {

            JavaPairRDD<String, String> files = sc.wholeTextFiles("./data/*.csv");
            files.cache();
            long numFiles = files.count();

            JavaRDD<String> linesWithFileName = splitLinesAndAddFileName(files);
            JavaRDD<Input> input = linesWithFileName.map(new InputMapper());

            LookForPreviousValues transform = new LookForPreviousValues(1000);
            JavaRDD<Output> out = transform.lookForPreviousValues(input);


            out.keyBy(Output::getPartitionFile)
                    .repartitionAndSortWithinPartitions(new HashPartitioner((int) numFiles))
                    .values()
                    .keyBy(Output::getNumber)
                    .sortByKey()
                    .values()
                    .saveAsTextFile("output/");
        }
    }

    @NotNull
    private static JavaSparkContext createSparkContext() {
        SparkConf conf = new SparkConf().setAppName("example").setMaster("local[8]");
        return new JavaSparkContext(conf);
    }


    private static JavaRDD<String> splitLinesAndAddFileName(JavaPairRDD<String, String> files) {
        return files.flatMap(a -> a._2.lines().map(l -> a._1 + "," + l)
                .iterator());
    }
}