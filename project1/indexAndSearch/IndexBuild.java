package indexAndSearch;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import indexAndSearch.WordCount.Map;
import indexAndSearch.WordCount.Reduce;

public class IndexBuild {
	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] ans = line.split("-");
			Text t1 = new Text();
			Text t2 = new Text();
			t1.set(ans[0]+"-");
			t2.set(ans[1]);
			output.collect(t1, t2);
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text,Text,Text,Text>{
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
			String ans = "";
			while(values.hasNext()) {
				ans += values.next().toString();
			}
			
			Text t = new Text();
			t.set(ans);
			output.collect(key, t);
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("indexBuild");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//conf.setOutputFormat(MultipleTextOutputFormat.class); 
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
