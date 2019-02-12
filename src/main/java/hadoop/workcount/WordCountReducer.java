package hadoop.workcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther Chris Lee
 * @Date 2/12/2019 16:56
 * @Description 进行后并后数据的最终统计
 * Text:Mapper输出的文本内容
 * IntWritable：Mapper的输出个数
 * Text：Reducer的输出文本
 * IntWritable：Reducer的输出文本
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
