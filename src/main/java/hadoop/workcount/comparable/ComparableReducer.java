package hadoop.workcount.comparable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther Chris Lee
 * @Date 3/28/2019 16:47
 * @Description
 */
public class ComparableReducer extends Reducer<KeyComparator, IntWritable, Text, IntWritable> {
	
	public void reduce(KeyComparator key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
		for (IntWritable val : values) {
			context.write(new Text(key.getKey()), val);
		}
	}
}
