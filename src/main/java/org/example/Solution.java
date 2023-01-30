package org.example;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.example.mapper.InputMapper;
import org.example.model.Input;
import org.example.model.Output;

public class Solution {


    public void solve(SparkSession sparkSession, String inputDirectory, String outputDirectory) {
        try (JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())) {

            JavaPairRDD<String, String> files = sc.wholeTextFiles(inputDirectory);
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
                    .saveAsTextFile(outputDirectory);
        }
    }

    private static JavaRDD<String> splitLinesAndAddFileName(JavaPairRDD<String, String> files) {
        return files.flatMap(a -> a._2.lines().map(l -> a._1 + "," + l)
                .iterator());
    }

}
