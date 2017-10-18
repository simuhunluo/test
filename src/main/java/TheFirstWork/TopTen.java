package TheFirstWork;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * 计算出每台风机每月份的风速TOP10。
 * 输出格式 ：
 * 风机类型1 月份1 风速第1名
 * 。。。
 * 风机类型1 月份12 风速第10名
 *
 * 风机类型2 月份1 风速第1名
 * 。。。
 * 风机类型2 月份12 风速第10名
 *
 */
public class TopTen {
    public static class GroupTopKMapper extends
            Mapper<LongWritable, Text, Text, DoubleWritable> {

        private String accurateTime="";
        private String total="";
        private FanData fanData = new FanData();
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            fanData.getInstance(value.toString());
            accurateTime=fanData.getTime();
            String[] times=accurateTime.split(" ");
            String fanNo=fanData.getFanNo();
            String[] time=times[0].split("-");
            String month=time[1];
            total=fanNo+"_"+month;
            context.write(new Text(total), new DoubleWritable(Double.parseDouble(fanData.getWindSpeed())));
        }
    }

    public static class GroupTopKReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context) throws IOException, InterruptedException {
            TreeSet<Double> ts = new TreeSet<Double>();
            for (DoubleWritable val : values) {
                ts.add(val.get());
                if (ts.size() > 10) {
                    ts.remove(ts.first());
                }
            }
            for (Double score : ts) {
                context.write(key, new DoubleWritable(score));
            }
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.addResource("../resources/core-site.xml");
        conf.addResource("../resources/hdfs-site.xml");
        HDFSUtils hdfs = new HDFSUtils(conf);
        hdfs.deleteDir(args[1]);
        Job job = Job.getInstance();
        job.setJarByClass(TopTen.class);
        job.setMapperClass(GroupTopKMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setReducerClass(GroupTopKReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
