package XMLParser;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by leon on 10/21/16.
 */
public class PageLinkReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        List<Text> linkArray = new ArrayList<Text>();
//        for (Text value: values) {
//            linkArray.add(value);
//        }
//
//        String[] links = new String[linkArray.size()];
//        int i = 0;
//        for (Text link: linkArray) {
//            links[i ++] = link.toString();
//        }
//
//        ArrayWritable linksWritable = new ArrayWritable(links);
//
//        context.write(key, linksWritable);
        String links = "\t";
        boolean first = true;
        for (Text value: values) {
            if (!first) links += ",";
            links += value.toString();
            first = false;
        }
        context.write(key, new Text(links));
    }
}
