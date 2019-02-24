package masterdataset;

import com.backtype.hadoop.pail.SequenceFileFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataStore {
    public static FileSystem configureHDFS() {
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000/user/luca");
            conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "false"); // TODO: 22/02/19 MAYBE in a cluster with multiple nodes must be true
            conf.setBoolean("dfs.support.append", true);
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            System.out.println("Unable to configure fle system!");
        }
        return fs;
    }

    public static FileSystem createAppendHDFS(FileSystem fs, String filePath, String tweet) throws IOException {
        Path hdfsPath = new Path(filePath);
        FSDataOutputStream out;
        Boolean isAppendable = Boolean.valueOf(fs.getConf().get("dfs.support.append"));
        PrintWriter writer;
        if (isAppendable) {
            if (fs.exists(hdfsPath)) {
                out = fs.append(hdfsPath);
                writer = new PrintWriter(out);
                writer.append(tweet + "\n");
            } else {
                out = fs.create(hdfsPath);
                writer = new PrintWriter(out);
                writer.append(tweet + "\n");
            }
            writer.flush();
            out.hflush();
            writer.close();
            out.close();
        } else
            System.out.println("----------File " + filePath + " isn't appendable----------");
        return fs;
    }

    public static FileSystem createAppendHDFS(FileSystem fs, String filePath, List<List> tweet) throws IOException {
        for (int i = 0; i < tweet.size(); i++) {
            createAppendHDFS(fs, filePath, tweet.get(i).get(0).toString());
        }
        return fs;
    }

    public static List readFromHdfs(FileSystem fileSystem, String filePath) {
        Path hdfsPath = new Path(filePath);
        List fileContent = new ArrayList();
        try {
            BufferedReader bfr = new BufferedReader(new InputStreamReader(fileSystem.open(hdfsPath)));
            String str;
            while ((str = bfr.readLine()) != null) {
                fileContent.add(Arrays.asList(str));
            }
        } catch (IOException ex) {
            System.out.println("----------Could not read from HDFS " + filePath + "---------");
        }
        return fileContent;
    }

    public static void deleteFromHdfs(FileSystem fs, String filePath) {
        try {
            if (fs.exists(new Path(filePath)))
                fs.delete(new Path(filePath));
        } catch (IOException e) {
            System.out.println("----------Unable to delete " + filePath + "----------");
        }
    }

    public static void closeHDFS(FileSystem fs) {
        try {
            fs.close();
        } catch (IOException e) {
            System.out.println("----------Unable to close  file system----------");
        }
    }
}