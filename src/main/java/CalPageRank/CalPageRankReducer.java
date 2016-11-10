package CalPageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Random;

/**
 * Created by leon on 11/3/16.
 */
public class CalPageRankReducer extends Reducer<Text, Text, Text, Text> {
    private static final float damping = 0.85F;

    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isExistingWikiPage = false;
        String[] split;
        float sumShareOtherPageRanks = 0;
        String links = "";
        String pageWithRank;

        for (Text value: values) {
            pageWithRank = value.toString();

            if(pageWithRank.equals("!")) {
                isExistingWikiPage = true;
                continue;
            }

            if(pageWithRank.startsWith("|")){
                links = "\t"+pageWithRank.substring(1);
                continue;
            }

            split = pageWithRank.split("\\t");

            float pageRank = Float.valueOf(split[1]);
            int countOutLinks = Integer.valueOf(split[2]);

            sumShareOtherPageRanks += (pageRank/countOutLinks);
        }

        if (!isExistingWikiPage) return;
        Random rn = new Random();
        float newRank = damping * sumShareOtherPageRanks + (1 - damping) + Math.abs(rn.nextInt() % 100);

        context.write(page, new Text(newRank + links));
    }
}
