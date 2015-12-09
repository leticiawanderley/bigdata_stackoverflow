package models;

import java.io.*;

import org.apache.hadoop.io.*;

public class IntArrayPair implements WritableComparable<IntArrayPair> {

	private IntWritable first;
	private ArrayWritable second;

	public IntArrayPair() {
		String[] s = new String[0];
		set(new IntWritable(), new ArrayWritable(s));
	}

	public IntArrayPair(int first, String[] second) {
		set(new IntWritable(first), new ArrayWritable(second));
	}

	public void set(IntWritable first, ArrayWritable second) {
		this.first = first;
		this.second = second;
	}

	public IntWritable getFirst() {
		return first;
	}

	public ArrayWritable getSecond() {
		return second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof IntArrayPair) {
			IntArrayPair tp = (IntArrayPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	@Override
	public String toString() {
		return first + "\t" + ArrayToString(second);
	}
	
	private String ArrayToString(ArrayWritable second){
		String[] items = second.toStrings();
		String result = "";
		
		for(String item : items){
			result = result + item + ";";
		}
		return result;
	}

	@Override
	public int compareTo(IntArrayPair tp) {
		int cmp = first.compareTo(tp.first);
		return cmp;
	}
}
