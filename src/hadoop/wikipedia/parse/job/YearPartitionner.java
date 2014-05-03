package hadoop.wikipedia.parse.job;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;



public class YearPartitionner extends Partitioner<LongWritable,Text> implements Configurable {

	
	private static final String FIRST_YEAR_MODIF = "first.year";
	private Configuration conf = null; 
	private long firstYear =  0;

	@Override
	public int getPartition(LongWritable key, Text value, int numReduceTasks) {
		return (int) (key.get() - firstYear);
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		firstYear = conf.getInt(FIRST_YEAR_MODIF, 0);
		}
		
	public static void setFirstYear(Job job, int firstYear) {
		System.out.println(firstYear);
        job.getConfiguration().setInt(FIRST_YEAR_MODIF,firstYear);
}

}
