package RankPages;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by leon on 11/10/16.
 */
public class RankPageReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {
    public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float parseFloat = key.get() * (-1);
        for (Text value: values) {
            context.write(value, new FloatWritable(parseFloat));
        }
    }
}
