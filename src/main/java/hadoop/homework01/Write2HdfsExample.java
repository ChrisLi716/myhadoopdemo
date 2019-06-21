package hadoop.homework01;

import hadoop.CommonUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
/**
 * @Auther Chris Lee
 * @Date 3/21/2019 11:05
 * @Description
 */
public class Write2HdfsExample {

    private static void mkdirInHdfs(String path) throws Exception {
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            Path myPath = new Path(path);
            fs = myPath.getFileSystem(conf);
            System.out.println("Creating " + path + " on hdfs...");

            // First create a new directory with mkdirs
            fs.mkdirs(myPath);
            System.out.println("Create " + path + " on hdfs successfully.");
        } catch (Exception e) {
            System.out.println("Exception:" + e);
        } finally {
            CommonUtils.closeFileSystem(fs);
        }
    }


    private static void writeContent2HDFS(final String dirPath, final String fileName, final byte[] content) {
        FileSystem fs = null;
        FSDataOutputStream outputStream = null;
        try {
            Configuration configuration = new Configuration();
            fs = FileSystem.get(configuration);
            Path path = new Path(dirPath + "/" + fileName);
            System.out.println("separator:" + File.separator);

            outputStream = fs.create(path);
            outputStream.write(content);
            outputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            CommonUtils.closeOutputStream(outputStream);
            CommonUtils.closeFileSystem(fs);
        }
    }


    public static void main(String[] args) {
        try {
            System.setProperty("HADOOP_USER_NAME", "bigdata");
            String dirPath = "hdfs://hadoop:9000/aa/test/";
            String fileName = "1.txt";
            String content = "hello world";
            mkdirInHdfs(dirPath);
            //writeContent2HDFS(dirPath, fileName, content.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            System.out.println("Exceptions:" + e);
        }
        System.out.println("timestamp:" + System.currentTimeMillis());
    }
}