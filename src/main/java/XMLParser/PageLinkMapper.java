package XMLParser;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by leon on 10/21/16.
 */
public class PageLinkMapper extends  Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern wikiLinksPattern = Pattern.compile("\\[.+?\\]");
    private static String title;
    private static String text;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        fetchTitleAndText(value);

        if (validString(title) == false) return;

        // replace the " " with "_" in the title
        Text titleText = new Text(title.replace(" ", "_"));

        // get the outgoing links
        Matcher matcher = wikiLinksPattern.matcher(text);

        while (matcher.find()) {
            String link = matcher.group();
            link = wikiLink(link);
            if (link == null || link.equals("")) {
                continue;
            }
            context.write(titleText, new Text(link));
        }
    }

    private void fetchTitleAndText(Text value) throws CharacterCodingException {
        title = text = "";

        int start = value.find("<title>");
        int end = value.find("</title>", 0);

        if (start == -1 || end == -1) return;
        start += 7;

        // fetch title
        title = Text.decode(value.getBytes(), start, end - start);

        start = value.find("<text");
        end = value.find("</text>", 0);

        // no out links
        if (start == -1 || end == -1) return;
        start += 6;
        // fetch text
        text = Text.decode(value.getBytes(), start, end - start);
    }

    private String wikiLink(String link) {
        if (validWikiLink(link) == false) return null;

        int start = link.startsWith("[[") ? 2 : 1;
        int end = link.indexOf("]");

        int pipePosition = link.indexOf("|");
        if (pipePosition > 0) end = pipePosition;

        int part = link.indexOf("#");
        if (part > 0) end = part;

        link = link.substring(start, end);
        link = link.replaceAll("\\s", "_");
        link = link.replace(",", "");
        link = link.replace("&amp", "&");

        return link;
    }

    private boolean validString(String s) {
        return !s.contains(":");
    }

    private boolean validWikiLink(String link) {
        int start = 1;
        if (link.startsWith("[[")){
            start = 2;
        }

        if (link.length() < start+2 || link.length() > 100) return false;
        char firstChar = link.charAt(start);

        if (firstChar == '#') return false;
        if (firstChar == ',') return false;
        if (firstChar == '.') return false;
        if (firstChar == '&') return false;
        if (firstChar == '\'') return false;
        if (firstChar == '-') return false;
        if (firstChar == '{') return false;

        if (link.contains(":")) return false;
        if (link.contains(",")) return false;
        if (link.contains("&")) return false;

        return true;
    }
}
