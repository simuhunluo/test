package AVGWindSpeed;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * SELECT b.月份,b.fanNo,avg(b.ws) FROM fanData b group by b.月份,b.fanNo。
 *计算月平均风速
 */
public class AVGWindSpeed {

    public static class AVGWindSpeedMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private FanData fanData = new FanData();
        private String month="";
        private String fanNo="";
        private Double ws=0.0;
        private List<Map<String,Double>> list=new ArrayList<Map<String, Double>>();
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            fanData.getInstance(value.toString());
            ws=Double.parseDouble(fanData.getWindSpeed());
            String accurateTime=fanData.getTime();
            String[] times=accurateTime.split(" ");
            fanNo=fanData.getFanNo();
            String[] time=times[0].split("-");
            month=time[1];
            Map<String,Double> map=new HashMap<String, Double>();
            map.put(month+"_"+fanNo,ws);
            list.add(map);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            Iterator<Map<String,Double>> it=list.iterator();
            while (it.hasNext()){//数据清洗
                Map<String,Double> map=it.next();
                for(Map.Entry<String, Double> vo : map.entrySet()){
                    if (vo.getValue()>=0){
                        context.write(new Text(vo.getKey()),new DoubleWritable(vo.getValue()));
                    }
                }
            }
        }
    }

    static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            int count=0;
            Double sum=0.0;
            for (DoubleWritable dw :values) {
                sum+=dw.get();
                count++;
            }
            if (count==0){//非零躲避
                context.write(key,new DoubleWritable(0));
            }else{
                Double avg=sum/count;
                context.write(key,new DoubleWritable(avg));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        HDFSUtils hdfs = new HDFSUtils(conf);
        hdfs.deleteDir(args[1]);
        String[] otherArgs = args;
        if (otherArgs.length != 2) {
            System.err.println("Usage:Data Average <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Data Average");
        job.setJarByClass(AVGWindSpeed.class);
        job.setMapperClass(AVGWindSpeedMapper.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
