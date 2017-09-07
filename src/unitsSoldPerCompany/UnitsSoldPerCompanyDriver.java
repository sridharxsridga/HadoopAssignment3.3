/*
 * UnitsSoldPerCompanyDriver is a driver class which drives the execution of map reduce programs
 */
package unitsSoldPerCompany;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class UnitsSoldPerCompanyDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//if inputput and output path is not specified appropriately
		if(args.length < 2 || args.length > 2 ){
			System.out.println("Two arguments are required for Input and output path respectively");
			System.exit(0);
		}
		
		
		// To set job related configuration
		Configuration conf = new Configuration();												

		/*
		 *  creating a job which allows the user to configure the job, submit it, control its execution, and query the state
		 */
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "TaskofJob");
		
		//Set the Jar by finding where a given class came from UnitsSoldPerCompanyDriver
		job.setJarByClass(UnitsSoldPerCompanyDriver.class);
		
		
		/*FileInputFormat is the base class for all file-based InputFormats.Add a Path to the list of inputs for the map-reduce job by using addInputPath()
		 * Path names a file or directory in a FileSystem. Path strings use slash as the directory separator.
		 */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		/*
		 * FileOutputFormat is a base class for OutputFormats that read from FileSystems.
		 * Set the Path of the output directory for the map-reduce job by using method setOutputPath()
		 * Path names a file or directory in a FileSystem. Path strings use slash as the directory separator.
		 */
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//Set the mapper class
		job.setMapperClass(UnitsSoldPerCompanyMapper.class);
		
		//set the reducer class
		job.setReducerClass(UnitsSoldPerCompanyReducer.class);
		/*
		 * BasicConfigurator.configure();log4j was added since i was getting warning msg for log appenders as "WARN No appenders could be found for logger (org.apache.hadoop.metrics2.lib.MutableMetricsFactory)" 
		 */
		BasicConfigurator.configure();
		//Set the type of output of key from mapper class
		
		job.setMapOutputKeyClass(Text.class);
		//Set the type of output of value from mapper class
		job.setMapOutputValueClass(IntWritable.class);
		
		//Set the type of output of key from reducer class
		job.setOutputKeyClass(Text.class);
		//Set the type of output of value from reducer class
		job.setOutputValueClass(IntWritable.class);
		
		//Submit the job to the cluster and wait for it to finish.
     	job.waitForCompletion(true);

	}

}