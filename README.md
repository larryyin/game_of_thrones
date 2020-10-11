# Code challenge  - Game of Thrones

## Steps to follow
The best way to replicate the environment is to use Zeppelin docker which has notebook for scala and built-in Spark.
1. Make sure docker is installed. https://docs.docker.com/get-docker/
2. Download this repository to your local directory {yourPath}. Then cd {yourPath}.
3. Run Zeppelin docker:  
docker run -p 8080:8080 --rm -v $PWD/notebook:/notebook -v $PWD/data:/data -e ZEPPELIN_NOTEBOOK_DIR='/notebook' --name zeppelin apache/zeppelin:0.9.0
4. In your web browser, go to http://localhost:8080
5. See if there is an exisitng notebook "question2" under the Filter box. If not,  
"Import note" -> "Select JSON File/IPYNB File" -> "{yourPath}/question2.zpln".  
Open notebook "question2".
6. In the notebook, run the first box to get the output. You can view the output at    
{yourPath}/data/output/  
  
### Check output
If you would like to see the output directly just go to {yourPath}/data/output/, where it is already generated.  
Alternatively, you can view it online:  
https://raw.githubusercontent.com/larryyin/game_of_thrones/main/data/output/part-00000-07d80175-58c7-4eea-8602-432392560dcf-c000.csv  

## Code explanation
// Set data root path  
val dataPathRoot = "file:///data/"  
// Set dataset path  
val dataPath = dataPathRoot+"dataset/*"  
// Read all files at once and process them in dataframe  
val df = spark.sparkContext.wholeTextFiles(dataPath).toDF  
val output = df  
// Parse file paths into document IDs  
.select((substring_index(col("_1"), "/", -1)).cast("Int").alias("docId"),  
// To simplify, extract only alphabetic words and then explode  
        explode(split((regexp_replace(col("_2"), "[^a-zA-Z\\s]", "")),"\\s+")).alias("w"))  
// Remove leading special characters and change all to lower case  
.select(col("docId"), (lower(regexp_replace(col("w"),"^[^a-zA-Z]+",""))).alias("w"))  
// Remove empty strings  
.filter(col("w")=!="")  
// Group by word and collect document ID set and then sort  
.groupBy("w").agg(concat_ws(" ",sort_array(collect_set("docId"))).alias("docList"))  
// (Optional step, not required by question) sort words to make the output pretty  
.orderBy("w")  
// (Optional step, not required by question) keep output in one file   
.repartition(1)  
// Write to output file  
output.write.mode("overwrite").option("header","false").csv(dataPathRoot+"output")  

### == Physical Plan ==
Exchange RoundRobinPartitioning(1)  
+- *(3) Sort [w#13 ASC NULLS FIRST], true, 0  
   +- Exchange rangepartitioning(w#13 ASC NULLS FIRST, 200)  
      +- ObjectHashAggregate(keys=[w#13], functions=[collect_set(docId#8, 0, 0)], output=[w#13, docList#19])  
         +- Exchange hashpartitioning(w#13, 200)  
            +- ObjectHashAggregate(keys=[w#13], functions=[partial_collect_set(docId#8, 0, 0)], output=[w#13, buf#25])  
               +- *(2) Project [cast(substring_index(_1#3, /, -1) as int) AS docId#8, lower(regexp_replace(w#10, ^[^a-zA-Z]+, )) AS w#13]  
                  +- *(2) Filter ((NOT (lower(regexp_replace(w#10, ^[^a-zA-Z]+, )) = ) && NOT Contains(lower(regexp_replace(w#10, ^[^a-zA-Z]+, )), --)) && NOT (lower(regexp_replace(w#10, ^[^a-zA-Z]+, )) = -))  
                     +- Generate explode(split(regexp_replace(_2#4, [^a-zA-Z\s], ), \s+)), [_1#3], false, [w#10]  
                        +- *(1) SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, scala.Tuple2, true])._1, true, false) AS _1#3, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, scala.Tuple2, true])._2, true, false) AS _2#4]  
                           +- Scan file:///data/dataset/*[obj#2]  

## Discussion
If the source data is over 200GB and memory is not enough, what could we do?  
The code is optimized and scalable working with spark dataframe. After words explode, it uses the default level of parallelism, which can be adjusted based on the overall memory footprint and available number of executor-cores and memory for optimization. There are 2 memory bottlenecks, though. First, we don't mind if the overall data size is over 200GB, but an individual document has to be small enough to fit into the allocated core memory. The maximum size of a document now is only ~4MB, so this shouldn't be a problem assuming a core has at least 1G of memory. Second, the groupBy shuffle stage should be smaller than the overall available memory, otherwise it will give an out of memory error. Also, the size of a partition should be less than the core memory, otherwise it spills to disk. In summary, we do need sufficient memory to have optimal performance.  
  
If we don't have sufficient memory, we have some walkarounds. One version could be to process one document at a time. Duplicated words and special characters are removed so that individual size is significantly smaller. Then individual outputs can be reduced into one.  

