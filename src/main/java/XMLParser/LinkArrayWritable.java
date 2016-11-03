package XMLParser;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by leon on 10/31/16.
 */
public class LinkArrayWritable extends ArrayWritable {
    public LinkArrayWritable(Text[] texts) {
        super(Text.class, texts);
    }

    public Text[] get() {
        return (Text[])super.get();
    }

    public void write(DataOutput out) throws IOException {
        Text[] texts = get();
        for(int i = 0; i < texts.length; ++i) {
            texts[i].write(out);
        }
    }

    public String toString() {
        String result = "1.0\t";
        boolean first = true;
        Text[] texts = get();
        for (Text text: texts) {
            if (!first) result += ",";
            result += text.toString();
            first = false;
        }
        return result;
    }
}
