package hadoop.wikipedia.parse.job;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class WikiParseReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
	
	
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		for (Text t : values) {
			context.write(t, NullWritable.get());
		}	
	}

}
