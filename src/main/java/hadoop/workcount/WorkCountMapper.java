package hadoop.workcount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther Chris Lee
 * @Date 2/12/2019 16:37
 * @Description Object:输入数据的具体内容
 * Text:每行的文本数据
 * Text：每个单词分解后的统计结果
 * IntWritable：输入统计记录的结果
 */
public class WorkCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String lineContent = value.toString();
        String[] result = lineContent.split(" ");

        //循环每个单词而后进行数量的生成
        for (String s : result) {
            if (StringUtils.isNotEmpty(s)) {
                context.write(new Text(s), new IntWritable(1));
            }
        }
    }

}
