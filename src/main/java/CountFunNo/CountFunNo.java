package CountFunNo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * SELECT count(b.fanNo) FROM fanData b;
 * 统计funNo中风机的数量
 */
public class CountFunNo {
    public static class CountFunNoMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private FanData fanData = new FanData();
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            fanData.getInstance(value.toString());
            String funNo=fanData.getFanNo();
            word.set(funNo);
            context.write(word,one);
        }
    }

    public static class CountFunNoReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable val : values) {
                sum+=val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        HDFSUtils hdfs = new HDFSUtils(conf);
        hdfs.deleteDir(args[1]);
        Job job = Job.getInstance(conf, "CountFunNo");
        job.setJarByClass(CountFunNo.class);
        job.setMapperClass(CountFunNo.CountFunNoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(CountFunNo.CountFunNoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
