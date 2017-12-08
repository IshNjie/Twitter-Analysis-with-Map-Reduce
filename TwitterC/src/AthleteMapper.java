import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.Hashtable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Set;

public class AthleteMapper extends Mapper<Object, Text, Text, IntWritable> {
private Text data = new Text();
private IntWritable one = new IntWritable(1);

private Hashtable<String, String> athlete; // Calls the athlete Hashtable variable
// the two string fields will be the athlete name and their respective sport.

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] tweets = value.toString().split(";"); // Separate the data by semicolons

		if (tweets.length == 4 && tweets[2].length() <= 140){
// The string must have 4 objects (epoch time, tweets id, tweets and device)
// The length of the 3rd index (2) must be less than or equal to 140 characters
// as that is the maximum amount of characters a tweet can have at the given time. 

			Set<String> AthleteName = athlete.keySet(); 
			// AthleteName is a variable that holds the two fields from the Hashtable. 
			
			for(String name: AthleteName){
				
				if(tweets[2].contains(name)){
				
				data.set(name);
				context.write(data,one);
			}
}
		}
}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		athlete = new Hashtable<String, String>(); 

		FileSystem fs = FileSystem.get(context.getConfiguration());
		// We know there is only one cache file, coded in main java file), so we only retrieve that URI
		URI fileUri = context.getCacheFiles()[0];


		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {

			br.readLine();

			while ((line = br.readLine()) != null) {


				String[] fields = line.split(",");
				// Fields are: 0:ID 1:Name 2:Nationality 3:Sex 4:DOB 5:Height 6:Weight 7:Sport 8:Gold 9:Silver 10:Bronze
				if (fields.length == 11)
						athlete.put(fields[1], fields[7]);
			}

			br.close();

		} catch (IOException e1) {
			}

		super.setup(context);
	}
}
