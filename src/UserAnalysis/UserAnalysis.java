package UserAnalysis;

import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import models.TextTextPair;

enum CustomCounters {NUM_USERS}

public class UserAnalysis {
	
	public static void runJob(String[] input, String output) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance();

		job.setJarByClass(UserAnalysis.class);
		job.setMapperClass(UserAnalysisMapper.class);
		job.setReducerClass(UserAnalysisReduce.class);

		job.setMapOutputKeyClass(TextTextPair.class);
		job.setMapOutputValueClass(Text.class);
		
		//job.setNumReduceTasks(0);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.addCacheFile(new Path("/data/stackoverflow/Users").toUri());

		Path outputPath = new Path(output);
		FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		job.waitForCompletion(true);
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
	}

}
