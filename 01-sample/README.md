#### 测试`MapReduce`程序
```
$ hdfs dfs -cat input/*
java javascript lua go python rust nodejs typescript
java javascript lua go python rust nodejs typescript
C++ C F# R java rust

$ yarn jar jars/sample.jar com.zlikun.learning.WordCount input output
17/12/07 09:27:41 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
17/12/07 09:27:42 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
17/12/07 09:27:43 INFO input.FileInputFormat: Total input files to process : 1
17/12/07 09:27:43 INFO mapreduce.JobSubmitter: number of splits:1
17/12/07 09:27:43 INFO Configuration.deprecation: yarn.resourcemanager.system-metrics-publisher.enabled is deprecated. Instead, use yarn.system-metrics-publisher.enabled
17/12/07 09:27:43 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1512608762062_0002
17/12/07 09:27:44 INFO impl.YarnClientImpl: Submitted application application_1512608762062_0002
17/12/07 09:27:44 INFO mapreduce.Job: The url to track the job: http://zlikun-docker:8088/proxy/application_1512608762062_0002/
17/12/07 09:27:44 INFO mapreduce.Job: Running job: job_1512608762062_0002
17/12/07 09:27:55 INFO mapreduce.Job: Job job_1512608762062_0002 running in uber mode : false
17/12/07 09:27:55 INFO mapreduce.Job:  map 0% reduce 0%
17/12/07 09:28:02 INFO mapreduce.Job:  map 100% reduce 0%
17/12/07 09:28:10 INFO mapreduce.Job:  map 100% reduce 100%
17/12/07 09:28:11 INFO mapreduce.Job: Job job_1512608762062_0002 completed successfully
17/12/07 09:28:12 INFO mapreduce.Job: Counters: 49
	File System Counters
		FILE: Number of bytes read=142
		FILE: Number of bytes written=404019
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=249
		HDFS: Number of bytes written=88
		HDFS: Number of read operations=6
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=1
		Launched reduce tasks=1
		Data-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=4761
		Total time spent by all reduces in occupied slots (ms)=4872
		Total time spent by all map tasks (ms)=4761
		Total time spent by all reduce tasks (ms)=4872
		Total vcore-milliseconds taken by all map tasks=4761
		Total vcore-milliseconds taken by all reduce tasks=4872
		Total megabyte-milliseconds taken by all map tasks=4875264
		Total megabyte-milliseconds taken by all reduce tasks=4988928
	Map-Reduce Framework
		Map input records=3
		Map output records=22
		Map output bytes=215
		Map output materialized bytes=142
		Input split bytes=121
		Combine input records=22
		Combine output records=12
		Reduce input groups=12
		Reduce shuffle bytes=142
		Reduce input records=12
		Reduce output records=12
		Spilled Records=24
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=261
		CPU time spent (ms)=3020
		Physical memory (bytes) snapshot=494678016
		Virtual memory (bytes) snapshot=3947048960
		Total committed heap usage (bytes)=325582848
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=128
	File Output Format Counters 
		Bytes Written=88

$ hdfs dfs -cat output/*
C	1
C++	1
F#	1
R	1
go	2
java	3
javascript	2
lua	2
nodejs	2
python	2
rust	3
typescript	2
```