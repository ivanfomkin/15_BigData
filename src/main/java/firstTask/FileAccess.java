package firstTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class FileAccess {
    private Configuration configuration;
    private FileSystem hdfs;
    private String rootPath;

    /**
     * Initializes the class, using rootPath as "/" directory
     *
     * @param rootPath - the path to the root of HDFS,
     *                 for example, hdfs://localhost:32771
     */
    public FileAccess(String rootPath) throws IOException, URISyntaxException {
        configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");
        this.rootPath = rootPath;
        hdfs = FileSystem.get(
                new URI(rootPath), configuration
        );
    }

    /**
     * Creates empty file or directory
     *
     * @param path
     */
    public void create(String path) throws IOException {
        Path filePath = new Path(rootPath + "/" + path);

        if (hdfs.exists(filePath)) {
            System.out.println("Path " + filePath + " already exists!");
        } else {
            hdfs.createNewFile(filePath);
        }
    }

    /**
     * Appends content to the file
     *
     * @param path
     * @param content
     */
    public void append(String path, String content) throws IOException {
        Path filePath = new Path(rootPath + "/" + path);
        if (hdfs.exists(filePath) && hdfs.isFile(filePath)) {
            hdfs.setReplication(filePath, (short) 1);
            FSDataOutputStream file = hdfs.append(filePath);
            BufferedWriter writer = new BufferedWriter
                    (new OutputStreamWriter(file, "UTF-8"));

            writer.write(content);
            writer.flush();
            writer.close();
            file.close();
        } else {
            System.out.println("This is not file or this file does not exist");
        }
    }

    /**
     * Returns content of the file
     *
     * @param path
     * @return
     */
    public String read(String path) throws IOException {
        var result = new StringBuilder();
        Path filePath = new Path(rootPath + "/" + path);
        if (hdfs.isFile(filePath) && hdfs.exists(filePath)) {
            FSDataInputStream open = hdfs.open(filePath);
            hdfs.setReplication(new Path(rootPath), (short) 1);
            BufferedReader reader = new BufferedReader(new InputStreamReader(open));
//            reader.lines().forEach(l -> result.append(l));
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
            open.close();
            reader.close();
        } else {
            result.append("This is not file or this file does not exist");
        }
        return result.toString();
    }

    /**
     * Deletes file or directory
     *
     * @param path
     */
    public void delete(String path) throws IOException {
        Path filePath = new Path(rootPath + "/" + path);
        if (hdfs.exists(filePath)) {
            hdfs.delete(filePath, true);
        } else {
            System.out.println(path + " does not exist");
        }
    }

    /**
     * Checks, is the "path" is directory or file
     *
     * @param path
     * @return
     */
    public boolean isDirectory(String path) throws IOException {
        return hdfs.isDirectory(new Path(rootPath + "/" + path));
    }

    /**
     * Return the list of files and subdirectories on any directory
     *
     * @param path
     * @return
     */
    public List<String> list(String path) throws IOException {
        String prefix = "";
        if (!path.contains(rootPath)) prefix = rootPath + "/";
        Path filesPath = new Path(prefix + path);
        List<String> files = new ArrayList<>();
        if (hdfs.exists(filesPath)) {
            FileStatus[] fileStatuses = hdfs.listStatus(filesPath);
            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isDirectory()) {
                    files.addAll(list(fileStatus.getPath().toString()));
                } else {
                    files.add(fileStatus.getPath().toString());
                }
            }
        } else {
            System.out.println("Incorrect path!");
        }
        return files;
    }

    public void close() throws IOException {
        hdfs.close();
    }
}
