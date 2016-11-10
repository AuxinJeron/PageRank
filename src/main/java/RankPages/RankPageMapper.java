package RankPages;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.Random;

/**
 * Created by leon on 11/9/16.
 */
public class RankPageMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] pageAndRank = getPageAndRank(key, value);

        float parseFloat = Float.parseFloat(pageAndRank[1]) * (-1);

        Text page = new Text(pageAndRank[0]);
        FloatWritable rank = new FloatWritable(parseFloat);

        context.write(rank, page);
    }

    private String[] getPageAndRank(LongWritable key, Text value) throws CharacterCodingException {
        String[] pageAndRank = new String[2];
        int tabPageIndex = value.find("\t");
        int tabPageRankIndex = value.find("\t", tabPageIndex + 1);

        int end;
        if (tabPageIndex == - 1) {
            end = value.getLength() - (tabPageIndex + 1);
        }
        else {
            end = tabPageRankIndex - (tabPageIndex + 1);
        }

        pageAndRank[0] = Text.decode(value.getBytes(), 0, tabPageIndex);
        pageAndRank[1] = Text.decode(value.getBytes(), tabPageIndex + 1, end);

        return pageAndRank;
    }
}
