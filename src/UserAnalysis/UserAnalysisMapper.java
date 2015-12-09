package UserAnalysis;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Hashtable;
import java.util.Map;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import models.MRDPUtils;
import models.User;
import models.TextTextPair;

public class UserAnalysisMapper extends Mapper<Object, Text, TextTextPair, Text> {
	private Hashtable<Integer, User> userInfo;
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String xml = value.toString();

		if(xml != null){

			Map<String, String> map = MRDPUtils.transformXmlToMap(xml);

			if(map.containsKey("OwnerUserId")){
				Integer userId = Integer.parseInt(map.get("OwnerUserId"));
				if(userInfo.contains(userId)){
					User user = userInfo.get(userId);
					context.write(new TextTextPair(user.getUserName().toString(), user.getCreationDate().toString()), 
							new Text(map.get("CreationDate")));							
				}						
			}
		}

	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		userInfo = new Hashtable<Integer, User>();

		// We know there is only one cache file, so we only retrieve that URI
		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {
			// we discard the header row
			br.readLine();

			while ((line = br.readLine()) != null) {
				context.getCounter(CustomCounters.NUM_USERS).increment(1);
				Map<String, String> map = MRDPUtils.transformXmlToMap(line);
				if(map.containsKey("Id")){
					User user = new User();
					Integer id = Integer.parseInt(map.get("Id"));
					user.setUserId(new IntWritable(id));
					user.setUserName(new Text(map.get("DisplayName")));
					user.setCreationDate(new Text(map.get("CreationDate")));
					userInfo.put(id, user);
					}				
			}
			br.close();
		} catch (IOException e1) {
		}

		super.setup(context);
	}
}
