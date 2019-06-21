package hadoop.workcount.comparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @Auther Chris Lee
 * @Date 3/28/2019 16:56
 * @Description
 */
public class ComparableJob {
	
	public static void main(String[] args)
		throws Exception {
		// 一个完整的操作就称为一个作业即Job
		// 假设现在文件保存在HDSF上的"/input/info.txt"上，而且最终的输出结果也将保存在HDSF r "/out"目录中
		Configuration conf = new Configuration();
		// 考虑到我们最终需要使用HDFS进行内容的处理操作，并且输入时不带有HDFS地址
		String[] argArray = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = Job.getInstance(conf, "ComparableJob");
		job.setJarByClass(ComparableJob.class);
		
		job.setMapperClass(ComparableMapper.class);
		job.setMapOutputKeyClass(KeyComparator.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setGroupingComparatorClass(GroupingComparator.class);

		job.setReducerClass(ComparableReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(argArray[0]));
		FileOutputFormat.setOutputPath(job, new Path(argArray[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
