package firstTask;

import java.util.List;

public class Main {
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String HADOOP_CONTAINER_ID = "7ad107905ffc"; //Указать ID Docker-контейнера тут
    private static final String HADOOP_PATH = "hdfs://" + HADOOP_CONTAINER_ID + ":8020/";

    public static void main(String[] args) throws Exception {

        FileAccess hadoop = new FileAccess(HADOOP_PATH);
        hadoop.create("myFile.log");
        hadoop.create("myFile.txt");
        hadoop.create("testFolder");
        hadoop.create("testFolder");
        System.out.println("myFile.log is " + (hadoop.isDirectory("myFile.log") ? "directory" : "file"));
        System.out.println("myFile.txt is " + (hadoop.isDirectory("myFile.txt") ? "directory" : "file"));
        System.out.println("testFolder is " + (hadoop.isDirectory("testFolder") ? "directory" : "file"));

        hadoop.append("myFile.txt", "This is my file");
        System.out.println(hadoop.read("myFile.txt"));

        System.out.println("List of files:");
        hadoop.list(HADOOP_PATH).forEach(System.out::println);

        hadoop.delete("myFile.log");
        hadoop.delete("myFile.txt");
        hadoop.delete("testFolder");
        hadoop.delete("testFolder");

        hadoop.close();
    }

    private static String getRandomWord() {
        StringBuilder builder = new StringBuilder();
        int length = 2 + (int) Math.round(10 * Math.random());
        int symbolsCount = symbols.length();
        for (int i = 0; i < length; i++) {
            builder.append(symbols.charAt((int) (symbolsCount * Math.random())));
        }
        return builder.toString();
    }
}
