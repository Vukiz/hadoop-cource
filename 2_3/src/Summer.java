import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;



public class Summer
extends Reducer<Text, Text, Text, Text>
{
    public void reduce(Text key, Iterable<Text> values, Context context)
	throws IOException, InterruptedException
	{
        HashMap<Text, Integer>  prev_cur_map = new HashMap();
        Integer count;
        for (Text val : values)
        {
            count = prev_cur_map.get(val);
            if (count == null)
                prev_cur_map.put(val, new Integer(1));
            else
                prev_cur_map.put(val, count + 1);
        }

        Text maxText = new Text("");
        count = 0;
        for (Map.Entry<Text, Integer> entry : prev_cur_map.entrySet()) {
            if (entry.getValue() > count) {
                count = entry.getValue();
                maxText = entry.getKey();
            }
        }

        context.write(key, maxText);
    }
}
