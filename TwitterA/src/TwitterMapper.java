import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TwitterMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private IntWritable data = new IntWritable();
  	
    private int tweetLength;
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    
    	// epoch_time;tweetId;tweet(including #hashtags);device
    	
    	// Separate the data by semicolons
        String[] tweets = value.toString().split(";");
        
        if (tweets.length == 4 && tweets[2].length() <= 140) { 
        	// If string has 4 objects, assign 'tweetLength to the length of the second object
        	
        	tweetLength = tweets[2].length(); // Find the length of the tweet
          	        	
       // Formula for assigning tweet lengths to appropriate bins
        
         data.set(((tweetLength - (tweetLength - 1) % 5) + 4));
        
        context.write(data, one);
        
        }

        }

}
