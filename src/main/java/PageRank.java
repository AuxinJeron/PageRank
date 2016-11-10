import CalPageRank.CalPageRankMapper;
import CalPageRank.CalPageRankReducer;
import RankPages.RankPageMapper;
import RankPages.RankPageReducer;
import XMLParser.LinkArrayWritable;
import XMLParser.PageLinkMapper;
import XMLParser.PageLinkReducer;
import XMLParser.XmlInputFormat;
import org.apache.commons.configuration.event.ConfigurationErrorEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
        String xmlParsePath = args[1] + "/xml";
        String calculationPath = args[1] + "/cal";
        String pageRankPath = args[1] + "/rank";
        boolean completed = runXmlParsing(args[0], xmlParsePath);
        if (completed == false) return 1;
        completed = runRankCalculation(xmlParsePath, calculationPath);
        if (completed == false) return 1;
        completed = runRankOrder(calculationPath, pageRankPath);
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
        //xmlParser.setCombinerClass(PageLinkReducer.class);
        // Output / Reducer
        FileOutputFormat.setOutputPath(xmlParser, new Path(outputPath));
        xmlParser.setOutputFormatClass(TextOutputFormat.class);
        xmlParser.setOutputKeyClass(Text.class);
        xmlParser.setOutputValueClass(LinkArrayWritable.class);
        xmlParser.setReducerClass(PageLinkReducer.class);

        return xmlParser.waitForCompletion(true);
    }

    public boolean runRankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = getConf();

        Job rankCalculator = Job.getInstance(conf, "rankCalculator");
        rankCalculator.setJarByClass(PageRank.class);

        // Input / Mapper
        FileInputFormat.addInputPath(rankCalculator, new Path(inputPath));
        rankCalculator.setInputFormatClass(TextInputFormat.class);
        rankCalculator.setMapperClass(CalPageRankMapper.class);
        rankCalculator.setMapOutputKeyClass(Text.class);
        rankCalculator.setMapOutputValueClass(Text.class);

        // Output / Reducer
        FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));
        rankCalculator.setOutputFormatClass(TextOutputFormat.class);
        rankCalculator.setOutputKeyClass(Text.class);
        rankCalculator.setOutputValueClass(Text.class);
        rankCalculator.setReducerClass(CalPageRankReducer.class);

        return rankCalculator.waitForCompletion(true);
    }

    public boolean runRankOrder(String inputPath, String outputPath) throws  IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankOrdering = Job.getInstance(conf, "rankOrdering");

        rankOrdering.setJarByClass(PageRank.class);

        rankOrdering.setOutputKeyClass(Text.class);
        rankOrdering.setOutputValueClass(FloatWritable.class);
        rankOrdering.setReducerClass(RankPageReducer.class);

        rankOrdering.setMapperClass(RankPageMapper.class);
        rankOrdering.setMapOutputKeyClass(FloatWritable.class);
        rankOrdering.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));

        rankOrdering.setInputFormatClass(TextInputFormat.class);
        rankOrdering.setOutputFormatClass(TextOutputFormat.class);

        return rankOrdering.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PageRank(), args);
        System.exit(res);
    }
}
