package hadoop.homework01;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @Auther Chris Lee
 * @Date 3/21/2019 11:05
 * @Description
 */
public class ReadFromHdfsExample {
	
	private static void readContentFromHDFS(final String dirPath, final String fileName) {
		FileSystem fs = null;
		FSDataInputStream inputStream = null;
		try {
			Configuration configuration = new Configuration();
			fs = FileSystem.get(configuration);
			Path path = new Path(dirPath + Path.SEPARATOR + fileName);
			
			if (fs.exists(path)) {
				fs.delete(path, true);
			}
			
			inputStream = fs.open(path);
			byte[] content = new byte[inputStream.available()];
			if (-1 == inputStream.read(content)) {
				System.out.println("read file:" + path.getName() + " successfully!");
				System.out.println(new String(content));
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			if (null != inputStream) {
				try {
					inputStream.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			if (null != fs) {
				try {
					fs.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void main(String[] args) {
		try {
			String path = "/data/test/";
			String fileName = "1.txt";
			readContentFromHDFS(path, fileName);
		}
		catch (Exception e) {
			System.out.println("Exceptions:" + e);
		}
		System.out.println("timestamp:" + System.currentTimeMillis());
	}
}