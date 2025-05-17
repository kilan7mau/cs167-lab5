package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App2 {
    public static void main(String[] args) {
        final String inputPath = args[0];

        SparkConf conf = new SparkConf();

        // Nếu không có cấu hình master (vd: khi chạy bằng IDE hoặc java -jar), đặt mặc định là local[*]
        if (!conf.contains("spark.master")) {
            conf.setMaster("local[*]");
        }

        System.out.printf("Using Spark master '%s'\n", conf.get("spark.master"));
        conf.setAppName("CS167-Lab5");

        try (JavaSparkContext spark = new JavaSparkContext(conf)) {
            JavaRDD<String> logFile = spark.textFile(inputPath);
            System.out.printf("Number of lines in the log file: %d\n", logFile.count());
        }
    }
}
