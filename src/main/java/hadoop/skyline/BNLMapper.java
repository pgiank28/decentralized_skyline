package hadoop.skyline;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.util.*;

public class BNLMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{

	private int first = 0;
	private int parts;
	Random rnd=new Random();

	public void configure(JobConf jb){
		parts=Integer.parseInt(jb.get("partitions"));
	}

	public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> collector, Reporter reporter)
	throws IOException {

		if(first == 0){
			first = 1;
			return;
		}

		int partitions = rnd.nextInt(parts)+1;
		collector.collect(new IntWritable(partitions), value);
	}



}
