package indexAndSearch;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

public class MultipleTextOutputFormat <K extends WritableComparable, V extends Writable>  
extends MultipleOutputFormat<K, V>{
	private TextOutputFormat<K, V> theTextOutputFormat = null;
	@Override
	protected RecordWriter<K,V> getBaseRecordWriter(FileSystem fs, JobConf job, String name, Progressable arg3) throws IOException{
		if(theTextOutputFormat == null) {
			theTextOutputFormat = new TextOutputFormat<K,V>();
		}
		return theTextOutputFormat.getRecordWriter(fs, job, name, arg3);
	}
	
	@Override
	protected String generateFileNameForKeyValue(K key, V value, String name) {
		return name + "_"  + value.toString();
	}
}
