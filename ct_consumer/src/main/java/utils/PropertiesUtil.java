package utils;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
//
//public class PropertiesUtil {
//    public static Properties properties = null;
//    static{
//        try {
//            // 加载配置属性
//            InputStream inputStream = ClassLoader.getSystemResourceAsStream("hbaseconsumer.properties");
//            properties = new Properties();
//            properties.load(inputStream);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static String getProperty(String key){
//        return properties.getProperty(key);
//    }
//}
public class PropertiesUtil {
    public static Properties properties = null;
    static{
        try {
            // 加载配置属性
            InputStream inputStream = ClassLoader.getSystemResourceAsStream("hbaseconsumer.properties");
            properties = new Properties();
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key){
        return properties.getProperty(key);
    }
}
