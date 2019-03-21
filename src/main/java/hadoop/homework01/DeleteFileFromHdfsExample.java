package hadoop.homework01;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @Auther Chris Lee
 * @Date 3/21/2019 11:05
 * @Description
 */
public class DeleteFileFromHdfsExample {
	
	private static void deleteFileFromHdfs(String path, String fileName) {
		Configuration conf = new Configuration();
		Path myPath = new Path(path + File.pathSeparator + fileName);
		System.out.println("Deleting " + myPath.getName() + " on hdfs...");
		try (FileSystem fs = myPath.getFileSystem(conf)) {
			fs.delete(myPath, true);
			System.out.println("Deleting " + myPath.getName() + " on hdfs successfully.");
		}
		catch (Exception e) {
			System.out.println("Exception:" + e);
		}
	}
	
	public static void main(String[] args) {
		try {
			String path = "/data/test/";
			String fileName = "1.txt";
			deleteFileFromHdfs(path, fileName);
		}
		catch (Exception e) {
			System.out.println("Exceptions:" + e);
		}
		System.out.println("timestamp:" + System.currentTimeMillis());
	}
}