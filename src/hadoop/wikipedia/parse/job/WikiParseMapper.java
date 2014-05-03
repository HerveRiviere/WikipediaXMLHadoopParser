/**
 * Wikipedia title words hit hadoop job 
 * Author : Herv?? RIVIERE
 * Date : 30/03/14
 * Hadoop mapper class
 * 
 * Hadoop API used : mapreduce
 * one mapper per couple of <doc></doc>
 * Single reducer
 *  
 */

package hadoop.wikipedia.parse.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WikiParseMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private String title = null;
	private String timestamp = null;
	private String username = null;
	private String size = null;
	private String minor = null;
	private String comment = null;
	private String typeUser = null;

	@Override
	public void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {

		String page = value.toString();
		int startPositionTile = page.indexOf("<title>");
		if(startPositionTile>-1){
			
		
		int stopPositionTile = page.indexOf("</title>");
		title = clean(page.substring(startPositionTile + 7, stopPositionTile));
		
		String[] listRevision = page.split("<revision>");
		if(listRevision.length>1){
		for (int i = 1; i < listRevision.length; i++) {
			cleanVariable();
			int startPositionTimestamp = listRevision[i].indexOf("<timestamp>");
			if(startPositionTimestamp>-1){
			int stopPositionTimestamp = listRevision[i].indexOf("</timestamp>");
			timestamp = cleanTimestamp(listRevision[i].substring(
					startPositionTimestamp + 11, stopPositionTimestamp));
			}else timestamp="2000";
			int startPositionUserName = listRevision[i].indexOf("<username>");
			if (startPositionUserName> -1) {
			int stopPositionUserName = listRevision[i].indexOf("</username>");
			username = clean(listRevision[i].substring(	startPositionUserName + 10, stopPositionUserName));
			if(listRevision[i].toLowerCase().indexOf("bot")>-1) typeUser="bot";
			else typeUser="user";
			}else if(listRevision[i].indexOf("<ip>")>-1){
				startPositionUserName = listRevision[i].indexOf("<ip>");
				int stopPositionUserName = listRevision[i].indexOf("</ip>");
				username = listRevision[i].substring(startPositionUserName +4, stopPositionUserName);
				typeUser="ip";
			}else{
				username="";
				typeUser="none";
			}

			int startPositionComment = listRevision[i].indexOf("<comment>");
			if (startPositionComment > -1) {
				int stopPositionComment = listRevision[i].indexOf("</comment>");
				comment = clean(listRevision[i].substring(
						startPositionComment + 9, stopPositionComment));
			} else
				comment = "";

			String[] sizeSplit = listRevision[i].split("<text id=\"");
			if(sizeSplit.length>1){
			int startPositionSize = sizeSplit[1].indexOf("bytes=\"");
			int stopPositionSize = sizeSplit[1].indexOf("\" />");
			
			size = sizeSplit[1].substring(startPositionSize + 7,
					stopPositionSize);
			}else size="-1";
			if (listRevision[i].indexOf("<minor/>") > -1)
				minor = "minor";
			else
				minor = "normal";
			String outputTXT = title + "\t" + username + "\t" +typeUser+"\t" + size + "\t"+ minor + "\t" + comment+"\t"+timestamp;
			context.write(new LongWritable(Long.parseLong(timestamp.substring(0,4))), new Text(outputTXT));
		}
		}}

	}

	private String clean(String value) {

		value = value.replaceAll("\\s+", " ");

		return value;
	}

	private String cleanTimestamp(String timestamp) {

		String date = timestamp.substring(0, 10);
		String hour = timestamp.substring(11, 19);

		return date + " " + hour;
	}

	private void cleanVariable() {
		timestamp = null;
		username = null;
		size = null;
		minor = null;
		comment = null;
	}

}
