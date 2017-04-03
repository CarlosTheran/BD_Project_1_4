package Part4;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * Created by carlos on 03-24-17.
 */
public class TwitterRetweetsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String result = "[ ";
        int count = 0;

        for (Text value : values){
            result = result + value;
            result = result + " , ";
            count =count + 1;
        }

        Integer.toString(count);
        result = result + count;
        result = result + " ]";

        context.write(key, new Text(result ));
//5548
    }
}
