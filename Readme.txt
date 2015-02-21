Steps for execution :

1) Since I have used old version a few extra jars are needed 
2) To create a input directory use : hdfs dfs -mkdir input1
   To put the input file into directory : hdfs dfs -put users.dat input1
   To run the job : hadoop jar First.jar First input1 output1
3) To create a input directory use : hdfs dfs -mkdir input2
   To put the input file into directory : hdfs dfs -put users.dat input2
   To run the job : hadoop jar Second.jar Second input2 output2
4) To create a input directory use : hdfs dfs -mkdir input3
   To put the input file into directory : hdfs dfs -put movies.dat input3
   To run the job : hadoop jar Second.jar Second input3 output3