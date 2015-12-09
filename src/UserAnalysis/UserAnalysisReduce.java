package UserAnalysis;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import models.IntArrayPair;
import models.TextTextPair;

public class UserAnalysisReduce extends  Reducer <TextTextPair, Text, TextTextPair, IntArrayPair> {
	public void reduce(TextTextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Initiates the two counters
		ArrayList<String> dates = new ArrayList<String>();
		String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS";
		SimpleDateFormat dtf = new SimpleDateFormat(pattern);
		Date maxDate = new Date(Long.MIN_VALUE);
		Date minDate = new Date(Long.MAX_VALUE);

		// For each pair, sum the value of the first to have the total length
		// and the total of the second for the number of messages
		for (Text value : values) {
			String date = value.toString();
			Date dateTime;
			try {
				dateTime = dtf.parse(date);
				if(dateTime.after(maxDate)){
					maxDate = dateTime;
				}
				if(dateTime.before(minDate)){
					minDate = dateTime;
				}
				dates.add(date);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		String[] result = {minDate.toString(), maxDate.toString()};
		context.write(key,new IntArrayPair(dates.size(), result));
	}
}