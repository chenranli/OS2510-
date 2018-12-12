package indexAndSearch;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import indexAndSearch.IndexBuild.Map;
import indexAndSearch.IndexBuild.Reduce;

public class Search{
//	static String input_path = "";
//	static String output_path = "";
	static String i = "";
	
//	@Override
//	public int run(String[] args) throws IOException, URISyntaxException {
//		input_path = args[0];
//		output_path = args[1];
//		Configuration conf = new Configuration();
//		conf.set("keywords", args[2]);
//		final FileSystem fileSystem = FileSystem.get(new URI(input_path), conf);
//		final Path outPath = new Path(output_path);
//		if(fileSystem.exists(outPath)){
//			fileSystem.delete(outPath, true);
//		}
//		final JobConf job = new JobConf(conf);
//		job.setJarByClass(Search.class);
//		FileInputFormat.setInputPaths(job, input_path);
//		job.setInputFormat(TextInputFormat.class);
//		job.setMapperClass(Map.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(LongWritable.class);
//		FileOutputFormat.setOutputPath(job, outPath);
//		job.setOutputFormat(TextOutputFormat.class);
//		return 0;
//	}
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] ans = line.split("-");
			//Configuration conf = context.getConfiguration();
			//String keywords = conf.get("keywords");
			String[] check = i.split(",");
			for(String k:check) {
				if(ans[0].equals(k)) {
					System.out.println("here");
					Text t1 = new Text();
					Text t2 = new Text();
					t1.set(ans[0]);
					t2.set(ans[1]);
					output.collect(t1, t2);
				}
			}
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
		FileSystem fileSystem = FileSystem.get(new URI(args[0].toString()), new Configuration());
		 if (fileSystem.exists(new Path(args[1]))) {
		      fileSystem.delete(new Path(args[1]), true);
		  }
		i = args[2];
		JobClient.runJob(conf);
	}
}
