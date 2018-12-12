package indexAndSearch;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;

public class WordCount {
	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,PairWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(LongWritable key, Text value, OutputCollector<Text,PairWritable> output, Reporter reporter)throws IOException{
			String line = value.toString();
			System.out.println(line);
			StringTokenizer tokenizer = new StringTokenizer(line);
			String path = ((FileSplit)reporter.getInputSplit()).getPath().getName();
			while(tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken()+"-");
				PairWritable ans = new PairWritable(path,1);
				output.collect(word, ans);
			}
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text,PairWritable,Text,PairWritable>{
		public void reduce(Text key, Iterator<PairWritable> values, OutputCollector<Text,PairWritable> output, Reporter reporter)throws IOException{
			int sum = 0;
			//System.out.println(values.next().toString());
			HashMap<String,Integer> hm = new HashMap<String,Integer>();
			while(values.hasNext()) {
				PairWritable a = values.next();
				if(hm.containsKey(a.getStr1())) {
					sum = a.getInt2()+hm.get(a.getStr1());
					hm.put(a.getStr1(), sum);
				}else {
					hm.put(a.getStr1(), a.getInt2());
				}
				//output.collect(key, a);
			}
			//System.out.println(hm);
			for(Entry<String,Integer> t:hm.entrySet()) {
				PairWritable res = new PairWritable(t.getKey(),t.getValue());
				output.collect(key, res);
			}
			//System.out.println(output);
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(PairWritable.class);
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
