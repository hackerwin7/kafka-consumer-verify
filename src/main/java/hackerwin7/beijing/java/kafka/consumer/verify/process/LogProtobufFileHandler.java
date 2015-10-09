package hackerwin7.beijing.java.kafka.consumer.verify.process;

import hackerwin7.beijing.java.kafka.consumer.verify.consume.LogProtobufConsume;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by hp on 8/6/15.
 */
public class LogProtobufFileHandler {
    private static String zks = null;
    private static String topic = null;
    private static int partition = 0;
    private static long offset = 0;
    private static String filterStr = null;
    private static String filterStrData = null;
    private static long endOffset = Long.MAX_VALUE;

    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure(file2in("log4j.properties", "config.log4j"));
        initFile();
        LogProtobufConsume consume = new LogProtobufConsume();

        /*set filter*/
        consume.setFilterStr(filterStr);
        consume.setFilterStrData(filterStrData);

        /*start*/
        consume.start(zks, topic, partition, offset);
    }

    private static void initFile() throws Exception {
        InputStream is = file2in("log-protobuf-config.properties", "config.conf");
        Properties pro = new Properties();
        pro.load(is);
        zks = pro.getProperty("kafka.zookeeper");
        topic = pro.getProperty("kafka.topic");
        partition = Integer.valueOf(pro.getProperty("kafka.partition"));
        offset = Long.valueOf(pro.getProperty("kafka.offset").trim());
        filterStr = pro.getProperty("filter.header.key.value");
        filterStrData = pro.getProperty("filter.data.key.value");
        is.close();
    }

    public static InputStream file2in(String filename, String prop) throws Exception {
        String cnf = System.getProperty(prop, "classpath:" + filename);
        InputStream in = null;
        if(cnf.startsWith("classpath:")) {
            cnf = StringUtils.substringAfter(cnf, "classpath:");
            in = AvroFileHandler.class.getClassLoader().getResourceAsStream(cnf);
        } else {
            in = new FileInputStream(cnf);
        }
        return in;
    }
}
