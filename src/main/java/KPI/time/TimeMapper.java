package KPI.time;

import java.io.IOException;

import KPI.Kpi;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TimeMapper extends Mapper<Object, Text, Text, IntWritable>{

	private Text time = new Text();
	
	private IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Kpi kpi = Kpi.parse(value.toString());
		if(kpi.getIs_validate()) {
			time.set(kpi.getRequest_time());
			context.write(time, one);
		}
	}
}
