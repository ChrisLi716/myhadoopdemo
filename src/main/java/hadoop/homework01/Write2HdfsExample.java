package hadoop.homework01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @Auther Chris Lee
 * @Date 3/21/2019 11:05
 * @Description
 */
public class Write2HdfsExample {

    private static void mkdirInHdfs(String path) throws Exception {
        Configuration conf = new Configuration();
        Path myPath = new Path(path);
        System.out.println("Creating " + path + " on hdfs...");
        try (FileSystem fs = myPath.getFileSystem(conf)) {
            // First create a new directory with mkdirs
            fs.mkdirs(myPath);
            System.out.println("Create " + path + " on hdfs successfully.");
        } catch (Exception e) {
            System.out.println("Exception:" + e);
        }
    }

	
	private static void writeContent2HDFS(final String dirPath, final String fileName, final byte[] content) {
		FileSystem fs = null;
		FSDataOutputStream outputStream = null;
		try {
			Configuration configuration = new Configuration();
			fs = FileSystem.get(configuration);
			Path path = new Path(dirPath + Path.SEPARATOR + fileName);
			
			if (fs.exists(path)) {
				fs.delete(path, true);
			}
			
			outputStream = fs.create(path);
			outputStream.write(content);
			outputStream.flush();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			if (null != outputStream) {
				try {
					outputStream.close();
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
            String dirPath = "/data/test/";
            String fileName = "1.txt";
            String content = "hello world";

            mkdirInHdfs(dirPath);
			writeContent2HDFS(dirPath, fileName, content.getBytes(StandardCharsets.UTF_8));
		}
		catch (Exception e) {
			System.out.println("Exceptions:" + e);
		}
		System.out.println("timestamp:" + System.currentTimeMillis());
	}
}