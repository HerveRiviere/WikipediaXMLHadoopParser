/**
 * Wikipedia xml parser
 * Author : Herve RIVIERE
 * Date : 20/04/14
 * Hadoop job main class, launch the mapper and the partitioner
 * 
 * Hadoop API used : mapreduce
 * one mapper per couple of <page></page>
 * Single reducer
 * 
 *  The mapper use a override XmlInputFormat class as input format (from mahout project), 
 *  source can be found here : http://svn.apache.org/repos/asf/mahout/trunk/integration/src/main/java/org/apache/mahout/text/wikipedia/XmlInputFormat.java
 *  
 */
package hadoop.wikipedia.parse.job;

import hadoop.helper.xml.XmlInputFormat;

import java.util.Date;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class WikiParseDriver {

	public static void run(String[] args) throws Exception {

		
		 if (args.length != 2) {
		 System.err.println("File.jar fileLocation fileOutput");
		 ToolRunner.printGenericCommandUsage(System.err);
		 
		 } else {
		
		Date date = new Date();
		
		//Job cofiguration
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		conf.set("mapred.input.dir", args[0]);
		conf.set("mapred.output.dir", args[1] + date.getTime());
		conf.set("mapred.tasktracker.reduce.tasks.maximum","20");
		conf.set("mapred.reduce.tasks","15");
		//Job creation
		Job job = Job.getInstance(conf);

		// Set jar main class to locate it in the distributed environment
		job.setJarByClass(WikiParseDriver.class);
		// Set job name to locate it in the distributed environment
		job.setJobName("WikiParser");

		// Set input and output format, note that we use the overrided XmlInputFormat 
		//each record: text between xmlinput.start and xmlinput.end (whatever the xml structure !)
		job.setNumReduceTasks(15);//From 2000 to 2014 : 15 reducer
		YearPartitionner.setFirstYear(job, 2000);
		
		job.setPartitionerClass(YearPartitionner.class);
		
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(WikiParseReducer.class);
		job.setMapperClass(WikiParseMapper.class);
	

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		}
	}

	public static void main(String[] args) throws Exception {

		/*String[] argSample = new String[2];
		argSample[0] = "testFiles/";
		argSample[1] = "out";*/
		run(args);

	}

}
