# Lab 5

## Student 1 information

* Full name: - Hoàng Hữu Tiến Đạt
* E-mail: - dathht.21ad@vku.udn.vn
* Student ID: 21AD011
## Student 2 information
* Full name: Dương Tấn Huy
* E-mail:huydt.21ad@vku.udn.vn
* Student ID: 21AD025
## Student 3 information
* Full name: Lê Hồng Anh
* E-mail:anhlh.21ad@vku.udn.vn
* Student ID: 21AD002
## Answers

1. **(Q1) Do you think it will use your local cluster? Why or why not?**
* No, it will not use the cluster yet.
2. **(Q2) Does the application use the cluster that you started? How did you find out?**
* Yes, after changing the master URL to spark://laptop-k16jg0ef:8080/ (spark://192.168.1.2:7077), the application uses the Spark cluster.
3. **(Q3) What is the Spark master printed on the standard output on IntelliJ IDEA?**
* The Spark master printed on the standard output is: local[*].
* This means that the Spark application is running in local mode, which is typically used during development and testing. The local[*] setting tells Spark to run locally on the machine using all available CPU cores (the * represents all cores). This mode allows developers to test and debug their Spark programs directly within IntelliJ IDEA without the need for a full cluster setup.
4. **(Q4) What is the Spark master printed on the standard output on the terminal?**
- If running in local mode within an IDE, you'll often see local[*].
- When running on a cluster, the most important information is the Master's address, such as spark://192.168.1.2:7077 (this information can be configured and displayed in the terminal output, but the screenshots focus on the web UI)
5. **(Q5) For the previous command that prints the number of matching lines, how many tasks were created, and how much time it took to process each task.**
* Number of tasks: 2.
* Time per task: 
- Stage 0 tasks: The "Details for Stage 0 (Attempt 0)" show that the duration for the two tasks in this stage was around 0.3 seconds (or 300ms).
- Stage 1 tasks: The "Details for Stage 1 (Attempt 0)" show that the duration for the two tasks in this stage was approximately 60ms.
6. **(Q6) For the previous command that counts the lines and prints the output, how many tasks in total were generated?**
* Total tasks: 4.
7. **(Q7) Compare this number to the one you got earlier.**
* Q5 (count only): 2 tasks.
* Q6 (count + saveAsTextFile): 4 tasks.
* Comparison: The number of tasks in Q6 is double that of Q5 because Q6 performs both count and saveAsTextFile actions, each action creates 2 tasks, while Q5 only has count.
8. **(Q8) Explain why we get these numbers.**
* Number of partitions: The nasa_19950801.tsv file is divided into 2 partitions based on size (~200–300 MB) and HDFS block (128 MB). Each partition is processed by a task.
* The count action (Q5): Requires a full scan of the RDD to count matching rows, creating 2 tasks (one for each partition).
* The saveAsTextFile action (Q6): Requires a rescan of the RDD to write the results to HDFS, creating 2 more tasks.
* Total tasks (Q6): Since Spark performs each action independently and does not cache intermediate results, count and saveAsTextFile each create 2 tasks, resulting in a total of 4 tasks.
* Reason for 4 instead of 2: Spark rereads the file from scratch for each action since the RDD is not cached, resulting in the entire pipeline (reading and filtering) being executed twice.
9. **(Q9) What can you do to the current code to ensure that the file is read only once?**
* Solution: Use the cache() method to cache the matchingLines RDD after filtering, ensuring Spark only reads and processes the file once for both count and saveAsTextFile.
10. **(Q10) How many stages does your program have, and what are the steps in each stage?**
* Number of stages: 2
  * Steps in each stage:
     * Stage 0:
         * Step 1: textFile - Read the input file nasa_19950801.tsv into a JavaRDD<String>.
         * Step 2: mapToPair - Convert each line into a Tuple2<String, Integer> (key = response code, value = 1) by splitting the line and extracting the 6th column.
     * Stage 1:
        * Step 1: countByKey - Aggregate the JavaPairRDD to count the number of occurrences of each key (response code), creating a Map<String, Long>
11. **(Q11) Why does your program have two stages?**
* The program has two stages because the countByKey action triggers a shuffle operation.
* Explanation:
  * Stage 0 consists of the textFile and mapToPair transformations. These are "narrow" transformations, meaning they don't require data to be redistributed across partitions. Therefore, they can be executed within the same stage. textFile reads the data, and mapToPair transforms it into key-value pairs.
  * Stage 1 is initiated by the countByKey action. countByKey is a "wide" operation, necessitating a shuffle. A shuffle involves redistributing data across partitions so that all data with the same key is located on the same partition. This operation is computationally expensive and requires a distinct stage.
