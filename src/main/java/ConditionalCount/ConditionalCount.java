package ConditionalCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * SELECT COUNT(b.fanNo) From fanData b where b.ws > 4 and b.ws < 12;
 * 输出Text类型key int类型value
 * 输出格式：
 * WT02287  28730
 */
public class ConditionalCount {
    public static class DataChoiceMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private FanData fanData = new FanData();
        private Text word=new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            fanData.getInstance(value.toString());
            String funNo=fanData.getFanNo();
            double windspeed = Double.parseDouble(fanData.getWindSpeed());
            if(windspeed > 4 && windspeed <12) {
                word.set(funNo);
                context.write(word, one);
            }else {
                return;
            }
        }
    }

    public static class DataChoiceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
        Job job = Job.getInstance(conf, "ConditionalCount");
        job.setJarByClass(ConditionalCount.class);
        job.setMapperClass(ConditionalCount.DataChoiceMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(ConditionalCount.DataChoiceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
