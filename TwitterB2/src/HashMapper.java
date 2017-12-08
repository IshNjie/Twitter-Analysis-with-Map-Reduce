import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.regex.*;
import java.util.*;
import java.text.*;

public class HashMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    	// epoch_time;tweetId;tweet(including #hashtags);device

    	// Separate the data by semicolons
        String[] tweets = value.toString().split(";");

        if (tweets.length == 4) {
        	// If string has 4 objects
try{
        	String epoch_time = tweets[0]; // Assign the 0th object to epoch_time

          if (epoch_time.length() == 13){
            long epoch = Long.parseLong(epoch_time);// epoch time needs go from being a string to a long variable
            SimpleDateFormat sdf = new SimpleDateFormat("HH"); // Set a variable sdf to find the hour of the time emiited from the epoch time
            sdf.setTimeZone(TimeZone.getTimeZone("UTC")); // Time zones of tweets need to be considered, get the UTC equivalent to be able to make comparison
            String hour = sdf.format(epoch);

            
            if (Integer.parseInt(hour) == 1){ // epoch_time = string, hour should be integer and hard code to 1 as we know that 1am is the most popular hour
              Pattern p = Pattern.compile("(#\\w+)\\b"); // Regular Expression
              Matcher m = p.matcher(tweets[2].toLowerCase());
              while (m.find()){
                data.set(m.group());
                context.write(data, one);
              }
            }

          }
}
catch(Exception e){
  System.err.println("Error: " + e); // in case of any errors
}

        }
}
}
