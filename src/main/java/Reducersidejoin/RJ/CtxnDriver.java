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
public class CtxnDriver {

	 public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();
		 Job job = new Job(conf, "Reduce-side join");
		 job.setJarByClass(CtxnDriver.class);
		 job.setReducerClass(TxnReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, tmapper.class);

		 MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CtxnMapper.class);
		 Path outputPath = new Path(args[2]);
		  
		 FileOutputFormat.setOutputPath(job, outputPath);
		 outputPath.getFileSystem(conf).delete(outputPath);
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		 }
		 }
