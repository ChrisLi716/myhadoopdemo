package hadoop;

import org.apache.hadoop.fs.FileSystem;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public class CommonUtils {

    public static void closeFileSystem(FileSystem fs) {
        try {
            if (null != fs) {
                fs.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void closeInputStream(DataInputStream stream) {
        try {
            if (null != stream) {
                stream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void closeOutputStream(DataOutputStream stream) {
        try {
            if (null != stream) {
                stream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
