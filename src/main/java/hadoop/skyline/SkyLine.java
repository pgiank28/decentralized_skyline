package hadoop.skyline;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tuc.softnet.hadoop.skyline.SkyLine;
import tuc.softnet.hadoop.skyline.BNLMapper;
import tuc.softnet.hadoop.skyline.BNLReducer;

public class SkyLine{

	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();

		Path inputPath = new Path("hdfs://127.0.0.1:9000/"+args[0]);
		Path outputPath = new Path("hdfs://127.0.0.1:9000/tmp");

		JobConf job = new JobConf(conf, SkyLine.class);
		job.set("partitions", args[2]);
		job.set("dimensions", args[4]);
		job.setJarByClass(SkyLine.class);
		job.setJobName("Skyline implementation job");

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		int partitions = Integer.parseInt(args[2]);
		String split_method = args[3];

		if(partitions >10){
			System.out.println("Number of partitions is unacceptable");
			return;
		}

		if(split_method.equals("random")){
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormat(TextOutputFormat.class);

			job.setMapperClass(BNLMapper.class);
			job.setReducerClass(BNLReducer.class);
		}else{
			if(split_method.equals("angle")){
				job.setOutputKeyClass(IntWritable.class);
				job.setOutputValueClass(Text.class);
				job.setOutputFormat(TextOutputFormat.class);

				job.setMapperClass(angleMapper.class);
				job.setReducerClass(BNLReducer.class);
			}else{
				System.out.println("Splitting method unacceptable");
				return;
			}
		}


		RunningJob runningJob = JobClient.runJob(job);
		while(!runningJob.isComplete()){

		}

		Configuration conf2 = new Configuration();

		Path inPath = new Path(outputPath+"/part-00000");
		Path outPath = new Path("hdfs://127.0.0.1:9000/"+args[1]);

		JobConf jjob = new JobConf(conf, SkyLine.class);
		jjob.set("dimensions", args[4]);
		jjob.setJarByClass(SkyLine.class);
		jjob.setJobName("Skyline implementation job-2");

		FileInputFormat.setInputPaths(jjob, inPath);
		FileOutputFormat.setOutputPath(jjob, outPath);

		jjob.setOutputKeyClass(IntWritable.class);
		jjob.setOutputValueClass(Text.class);
		jjob.setOutputFormat(TextOutputFormat.class);

		jjob.setMapperClass(totalMapper.class);
		jjob.setReducerClass(BNLReducer.class);

		FileSystem hhdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf2);
		if (hhdfs.exists(outPath)){
		hhdfs.delete(outPath, true);}

		RunningJob rrunningJob = JobClient.runJob(jjob);
		while(!rrunningJob.isComplete()){

		}

		}



}
