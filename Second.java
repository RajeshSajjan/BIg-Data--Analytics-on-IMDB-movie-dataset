import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class Second {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>  {
    private final static IntWritable one = new IntWritable(1);
    private Text region = new Text();

    private String mytoken = null;
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] myarray = line.split("::");
      
	
  	if(Integer.parseInt(myarray[2]) <= 18)
	{
     mytoken = "7 "+myarray[1];
     region.set(mytoken);
     output.collect(region, one);
	 }
	 else if(Integer.parseInt(myarray[2]) > 18 && Integer.parseInt(myarray[2]) <= 24)
	 {
	 mytoken = "24 "+myarray[1];
     region.set(mytoken);
     output.collect(region, one);
	 }
	 else if(Integer.parseInt(myarray[2]) >= 25 && Integer.parseInt(myarray[2]) <= 34){
	  mytoken = "31 "+myarray[1];
     region.set(mytoken);
     output.collect(region, one);
	 }
	 else if(Integer.parseInt(myarray[2]) >= 35 && Integer.parseInt(myarray[2]) <= 44){
	  mytoken = "41 "+myarray[1];
     region.set(mytoken);
     output.collect(region, one);
	 }
	 else if(Integer.parseInt(myarray[2]) >= 45 && Integer.parseInt(myarray[2]) <= 55){
	  mytoken = "51 "+myarray[1];
     region.set(mytoken);
     output.collect(region, one);
	 }
	 else if(Integer.parseInt(myarray[2]) >= 56 && Integer.parseInt(myarray[2]) <= 61){
	  mytoken = "56 "+myarray[1];
     region.set(mytoken);
     output.collect(region, one);
	 }
	 else
	 {
	  mytoken = "62 "+myarray[1];
     region.set(mytoken);
     output.collect(region, one);
	 }
    }
  
 }

  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Second.class);
    
    conf.setJobName("regionCount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}

