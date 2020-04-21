package firstTask;

public class Main {
    private static String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String HADOOP_CONTAINER_ID = "56cccf022503"; //Указать ID Docker-контейнера тут
    private static final String HADOOP_PATH = "hdfs://" + HADOOP_CONTAINER_ID + ":8020/";

    public static void main(String[] args) throws Exception {


        FileAccess hadoop = new FileAccess(HADOOP_PATH);

        //Попробуем посоздавать файлы
        hadoop.create("myFile");
        hadoop.create("myfile/myfile");
        hadoop.create("test.txt");
        hadoop.create("myFile");
        //Выведем их список
        hadoop.list(HADOOP_PATH).forEach(System.out::println);

        //Проверим, дериктория ли это
        if (hadoop.isDirectory("myfile"))
            System.out.println("myFile is directory");
        else System.out.println("myFile is not directory");

        //Проверим методы read и append
        System.out.println(hadoop.read("test.txt"));
        hadoop.append("test.txt", "This is test text");
        System.out.println(hadoop.read("test.txt"));
        hadoop.append("test.txt", "This is second string");
        System.out.println(hadoop.read("test.txt"));

        //Удалим файлы
        hadoop.delete("abc");
        hadoop.delete("myFile");
        hadoop.delete("myfile/myfile");
        hadoop.delete("myfile");
        hadoop.delete("test");
        hadoop.delete("test.txt");

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
