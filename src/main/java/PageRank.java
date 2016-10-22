import XMLParser.PageLinkMapper;
import XMLParser.PageLinkReducer;
import XMLParser.XmlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by leon on 10/19/16.
 */

public class PageRank extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        boolean completed = runXmlParsing(args[0], args[1]);
        if (completed == false) return 1;
        return 0;
    }

    public boolean runXmlParsing(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = getConf();
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        Job xmlParser = Job.getInstance(conf, "xmlParser");
        xmlParser.setJarByClass(PageRank.class);

        // Input / Mapper
        FileInputFormat.addInputPath(xmlParser, new Path(inputPath));
        xmlParser.setInputFormatClass(XmlInputFormat.class);
        xmlParser.setMapperClass(PageLinkMapper.class);
        xmlParser.setMapOutputKeyClass(Text.class);
        xmlParser.setMapOutputValueClass(Text.class);
        xmlParser.setCombinerClass(PageLinkReducer.class);
        // Output / Reducer
        FileOutputFormat.setOutputPath(xmlParser, new Path(outputPath));
        xmlParser.setOutputFormatClass(TextOutputFormat.class);
        xmlParser.setOutputKeyClass(Text.class);
        xmlParser.setOutputValueClass(Text.class);
        xmlParser.setReducerClass(PageLinkReducer.class);

        return xmlParser.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PageRank(), args);
        System.exit(res);
    }
}
