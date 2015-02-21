import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Third {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>  {
    private final static IntWritable one = new IntWritable(1);
    private Text region = new Text();

    private static String gen; 
    private String mytoken = null;
    @Override
    public void configure(JobConf job) {
        super.configure(job);
        gen= job.get("third_args");
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] myarray = line.split("::");
      String[]  genre=myarray[2].split("\\|");
    	for(int i=0;i<=genre.length-1;i++)
    	{
    		if(genre[i].equalsIgnoreCase(gen))
    		{
    			mytoken=myarray[1];
    			region.set(mytoken);
    			output.collect(region, one);
    		}
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
    JobConf conf = new JobConf(Third.class);
    conf.set("third_args", args[2]);
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

