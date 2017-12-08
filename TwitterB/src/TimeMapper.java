import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.text.*;

public class TimeMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private IntWritable data = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    	// epoch_time;tweetId;tweet(including #hashtags);device

    	// Separate the data by semicolons
        String[] tweets = value.toString().split(";");

        if (tweets.length == 4) {
        	// If string has 4 objects

        	String epoch_time = tweets[0]; // Assign the 0th object to epoch_time

          if (epoch_time.length() == 13){
                long epoch = Long.parseLong(epoch_time);  // epoch time needs go from being a string to a long variable
            SimpleDateFormat sdf = new SimpleDateFormat("HH"); // Set a variable sdf to find the hour of the time emiited from the epoch time
            
            sdf.setTimeZone(TimeZone.getTimeZone("UTC")); 
            // Different time zones will depict different hours
            
            String hour = sdf.format(epoch);

            data.set(Integer.parseInt(hour)); // epoch_time = string, hour should be integer
           
            context.write(data, one);
          }


        }

}
}
