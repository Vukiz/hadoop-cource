package rstm.hadoop_course;

import java.io.*;
import org.apache.hadoop.io.*;



public class TextIntWritable implements WritableComparable<TextIntWritable>{


    private String word;
    private int frequency;

    public TextIntWritable(){

    }

    public TextIntWritable(String string, int value){

        word = new String (string);
        frequency = value;

    }


    public void write(DataOutput out) throws IOException {

        out.writeUTF(word);
        out.writeInt(frequency);

    }


    public void readFields(DataInput in)throws IOException
    {

        word = in.readUTF();
        frequency = in.readInt();

    }


    public String toString()
    {
        return (word + "\t" + Integer.toString(frequency));
    }



    public int compareTo( TextIntWritable tmp ) {
        int result;
        if( tmp == null ) throw new NullPointerException();
        else
            if( !(tmp instanceof TextIntWritable) ) throw new ClassCastException();
            else {
                TextIntWritable tmp2 = (TextIntWritable)tmp;
                if (this.frequency > tmp2.frequency) result = 1;
                else if(this.frequency == tmp2.frequency) result = 0;
                else result = -1;
            }
        return -result;


    }


    public int hashCode(){

        final int prime1 = 29;
        final int prime2 = 31;

        int result = prime1*this.frequency;
        result = result+this.word.length();

        return result;

    }


}