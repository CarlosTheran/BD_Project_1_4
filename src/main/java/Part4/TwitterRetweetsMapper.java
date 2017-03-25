package Part4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.*;

import java.io.IOException;
//import java.util.Scanner;

/**
 * Created by carlos on 03-24-17.
 */

public class TwitterRetweetsMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            // Long ID = status.getCurrentUserRetweetId();

            if (status.isRetweet())
            {
                Status originalTweet = status.getRetweetedStatus();
                long originaltweetid = originalTweet.getId();
                //long originaluserid = originalTweet.getUser().getId();

                context.write(new Text(Long.toString(originaltweetid)), new IntWritable(1));
            }

        }
        catch(TwitterException e){

        }

    }
}
