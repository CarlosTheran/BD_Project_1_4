package Part4;

import com.sun.jndi.url.dns.dnsURLContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.core.config.plugins.util.ResolverUtil;
import twitter4j.*;

import java.io.IOException;
//import java.util.Scanner;

/**
 * Created by carlos on 03-24-17.
 */

public class TwitterRetweetsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
           //


            if (status.isRetweet())
            {
                Status originalTweet = status.getRetweetedStatus();
                long originaltweetId = originalTweet.getId();
                Long ID = status.getId();
                //long originaluserid = originalTweet.getUser().getId();

                context.write(new Text(Long.toString(originaltweetId)), new Text(Long.toString(ID)));
            }

        }
        catch(TwitterException e){

        }

    }
}
