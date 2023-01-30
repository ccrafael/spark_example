package org.example;

import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        Solution solution = new Solution();
        solution.solve(SparkSession.builder()
                        .master("local[8]")
                .getOrCreate(), "data/*csv", "output/");
    }


}