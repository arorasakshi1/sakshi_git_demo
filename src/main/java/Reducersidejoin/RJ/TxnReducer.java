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

public  class TxnReducer extends Reducer <Text, Text, Text, Text>
{
public void reduce(Text key, Iterable<Text> values, Context context)
throws IOException, InterruptedException 
{
String name = "";
double total = 0.0;
int count = 0;
for (Text t : values) 
{ 
String parts3[] = t.toString().split("  ");
if (parts3[0].equals("tnxn")) 
{
count++;
total += Float.parseFloat(parts3[1]);
} 
else if (parts3[0].equals("cust")) 
{
name = parts3[1];
}
}
String str = String.format("%d %f", count, total);
context.write(new Text(name), new Text(str));
}
}
