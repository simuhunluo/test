package KPI.source;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SourceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	private IntWritable count = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0; 
		for(IntWritable value : values) {
			sum = sum + value.get();
		}
		count.set(sum);
		context.write(key, count);
	}

}
