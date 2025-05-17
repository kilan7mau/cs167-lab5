package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Filter {
    public static void main(String[] args) {
        // Kiểm tra số lượng tham số
        if (args.length < 1) {
            System.err.println("Sử dụng: Filter <inputPath> [outputPath] [desiredCode]");
            System.exit(1);
        }

        final String inputPath = args[0];
        final String desiredCode = args.length > 2 ? args[2] : "200";  // Mặc định là "200" nếu không được cung cấp

        // Replace [UCRNetID] with your own NetID
        try (JavaSparkContext spark = new JavaSparkContext("local[*]", "CS167-Lab5-Filter-[UCRNetID]")) {
            JavaRDD<String> logFile = spark.textFile(inputPath);

            // Filter lines based on the user-provided response code
            JavaRDD<String> matchingLines = logFile.filter(line -> line.split("\t")[5].equals(desiredCode));

            // Print the number of matching lines
            System.out.printf("The file '%s' contains %d lines with response code %s\n",
                    inputPath, matchingLines.count(), desiredCode);

            // Lưu kết quả nếu có đường dẫn đầu ra
            if (args.length > 1) {
                final String outputPath = args[1];
                matchingLines.saveAsTextFile(outputPath);
            }
        }
    }
}