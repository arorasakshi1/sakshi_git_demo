package Reducersidejoin.RJ;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;	
	 @SuppressWarnings("deprecation")
	 public class CtxnMapper extends Mapper<Object, Text, Text , Text>{
	 
		
		 
		@Override
		protected void map(Object key, Text value,
				Context context)
				throws IOException, InterruptedException {

			String record =value.toString();
			String parts[]=record.split(",");
			 context.write(new Text(parts[0]), new Text("cust   " + parts[1]));
		}
			
		
			
		

}


