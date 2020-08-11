package Reducersidejoin.RJ;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
/**
 * Hello world!
 *
 */
public class tmapper extends Mapper <Object, Text, Text, Text>
{
public void map(Object key, Text value, Context context) 
throws IOException, InterruptedException 
{
String record2 = value.toString();
String[] parts2 = record2.split(",");
context.write(new Text(parts2[1]), new Text("tnxn   " + parts2[2]));
}
}
