package KPI.pv;

import java.io.IOException;

import KPI.Kpi;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PVMapper extends Mapper<Object, Text, Text, IntWritable>{

	private IntWritable one = new IntWritable(1);
	
	private Text requestPage = new Text();
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Kpi kpi = Kpi.parse(value.toString());
		if(kpi.getIs_validate()) {
			requestPage.set(kpi.getRequest_page());
			context.write(requestPage, one);
		}
	}
}
