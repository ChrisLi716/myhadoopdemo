package hadoop.workcount.comparable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther Chris Lee
 * @Date 3/28/2019 16:40
 * @Description
 */
public class ComparableMapper extends Mapper<Text, Text, KeyComparator, IntWritable> {
	@Override
	protected void map(Text key, Text value, Context context)
		throws IOException, InterruptedException {
		String line = value.toString();
		String[] result = line.split(" ");
		for (String value : result) {
			context.write(new KeyComparator(key, value), new IntWritable(1));
		}
		
	}
}
