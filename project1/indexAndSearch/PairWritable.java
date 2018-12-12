package indexAndSearch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class PairWritable implements Writable {
    private Text value1;
    private IntWritable value2;

    public PairWritable() {
        value1 = new Text();
        value2 = new IntWritable();
    }

    public PairWritable(String value1, int value2) {
        this.value1 = new Text(value1);
        this.value2 = new IntWritable(value2);
    }

    public String getStr1() {
        return value1.toString();
    }

    public int getInt2() {
        return value2.get();
    }

    @Override
    public String toString() {
        return value1.toString()+" "+value2.toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value1.readFields(in);
        value2.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        value1.write(out);
        value2.write(out);
    }
}
