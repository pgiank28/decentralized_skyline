package hadoop.skyline;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class totalMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{


	public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> collector, Reporter reporter)
	throws IOException {

		StringTokenizer sg = new StringTokenizer(value.toString(),"\t");
		sg.nextToken();
		collector.collect(new IntWritable(1), new Text(sg.nextToken()));
	}
}
