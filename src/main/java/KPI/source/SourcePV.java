package KPI.source;

import KPI.HDFSUtils;
import KPI.SortKey;
import KPI.SortMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SourcePV {
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String inputPath = "/common/access.20120104.log";
		String outputPath = "/data/scc/kpi/source/";
		
		// 如果输出目录存在，则删除。
		HDFSUtils hdfs =new HDFSUtils(conf);
		hdfs.deleteDir(outputPath);
		
		Job job = Job.getInstance(conf, "pv-count");
		job.setJarByClass(SourcePV.class);
		job.setMapperClass(SourceMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(SourceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
		
		// 对计算结果做排序
		inputPath = outputPath + "part-*";
		outputPath = outputPath + "sort";
		job = Job.getInstance(conf, "pv-count_sort");
		job.setJarByClass(SourcePV.class);
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(SortKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
	
}
