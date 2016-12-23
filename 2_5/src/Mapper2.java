import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.analysis.tokenattributes.*;

import java.io.*;
import java.util.*;

import rstm.hadoop_course.TextIntWritable;

public class Mapper2
        //extends Mapper<Object, Text, TextIntWritable, Text> {
        extends Mapper<LongWritable, Text, TextIntWritable,Text> {


    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {




            String[] arr = value.toString().split("\t");


            int frequency = Integer.parseInt(arr[2]);
            TextIntWritable tiw = new TextIntWritable(arr[1], frequency);

            //context.write(new TextIntWritable(arr[1], Integer.parseInt(arr[2])), new Text(arr[0]));


            //context.write(new TextIntWritable("", 1), new Text(""));

            context.write(tiw, new Text(arr[0]));

    }


}