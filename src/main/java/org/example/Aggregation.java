package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.io.IOException;
import java.util.Map;

public class Aggregation {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: Aggregation <inputPath>");
            System.exit(1);
        }

        final String inputPath = args[0];
        SparkConf conf = new SparkConf();
        if (!conf.contains("spark.master"))
            conf.setMaster("local[*]");
        System.out.printf("Using Spark master '%s'\n", conf.get("spark.master"));
        conf.setAppName("CS167-Lab5");

        try (JavaSparkContext spark = new JavaSparkContext(conf)) {
            JavaRDD<String> logFile = spark.textFile(inputPath);

            // To do 1: Transform via `mapToPair`, return `Tuple2`
            JavaPairRDD<String, Integer> codes = logFile.mapToPair(line -> {
                try {
                    String[] parts = line.split("\t");
                    if (parts.length < 6) {
                        System.err.println("Invalid line: " + line);
                        return new Tuple2<>("Unknown", 1);
                    }
                    return new Tuple2<>(parts[5], 1);
                } catch (Exception e) {
                    System.err.println("Error processing line: " + line);
                    return new Tuple2<>("Error", 1);
                }
            });

            // To do 2: `countByKey`
            Map<String, Long> counts = codes.countByKey();

            // Print results
            for (Map.Entry<String, Long> entry : counts.entrySet()) {
                System.out.printf("Code '%s' : number of entries %d\n", entry.getKey(), entry.getValue());
            }

            System.in.read(); // Keep program running until input
        }
    }
}