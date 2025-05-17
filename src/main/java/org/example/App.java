package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App {
    public static void main(String[] args) {
        final String inputPath = args[0];
        // Replace [UCRNetID] with your own NetID
        try (JavaSparkContext spark = new JavaSparkContext("local[*]", "CS167-Lab5-App-[UCRNetID]")) {
            JavaRDD<String> logFile = spark.textFile(inputPath);
            System.out.printf("Number of lines in the log file %d\n", logFile.count());
        }
    }
}