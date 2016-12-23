package rstm.hadoop_course;

import java.io.*;
import org.apache.hadoop.io.*;



public class TextIntWritable implements Writable{


    private String word;
    private int frequency;

    public TextIntWritable(String string, int value){

        word = new String (string);
        frequency = value;

    }

    public void write(DataOutput out) throws IOException {
        //реализовать
        //с помощью релизованных в IntWritable и Text
        out.writeUTF(word);
        out.writeInt(frequency);

    }

    public void readFields(DataInput in)throws IOException
    {
        //реализовать
        //с помощью релизованных в IntWritable и Text
        word = in.readUTF();
        frequency = in.readInt();

    }



    public String toString()
    {
        return (word + ": " + Integer.toString(frequency));
    }


}