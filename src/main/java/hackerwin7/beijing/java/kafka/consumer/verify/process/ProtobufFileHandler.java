package hackerwin7.beijing.java.kafka.consumer.verify.process;

import hackerwin7.beijing.java.kafka.consumer.verify.consume.ProtobufConsume;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by hp on 5/26/15.
 */
public class ProtobufFileHandler {
    private static String zks = null;
    private static String topic = null;
    private static int partition = 0;
    private static long offset = 0;
    private static String filterDb = null;
    private static String filterTb = null;
    private static String priKey = null;
    private static String priVals = null;
    private static int fetchSize = 0;
    private static long endOffset = Long.MAX_VALUE;

    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure(file2in("log4j.properties", "config.log4j"));
        initFile();
        ProtobufConsume consume = new ProtobufConsume();
        consume.setDbname(filterDb);
        consume.setTbname(filterTb);
        consume.setpKey(priKey);
        consume.setpVals(priVals);
        consume.setEndOffset(endOffset);
        consume.setFetchSize(fetchSize);
        consume.start(zks, topic, partition, offset);
    }

    private static void initFile() throws Exception {
        InputStream is = file2in("protobuf-config.properties", "config.conf");
        Properties pro = new Properties();
        pro.load(is);
        zks = pro.getProperty("kafka.zookeeper");
        topic = pro.getProperty("kafka.topic");
        partition = Integer.valueOf(pro.getProperty("kafka.partition"));
        offset = Long.valueOf(pro.getProperty("kafka.offset"));
        filterDb = pro.getProperty("filter.dbname");
        filterTb = pro.getProperty("filter.tbname");
        priKey = pro.getProperty("primary.key");
        priVals = pro.getProperty("primary.values");
        String temp = pro.getProperty("kafka.offset.end");
        if(temp != null && !temp.trim().equals("")) {
            endOffset = Long.valueOf(temp);
        }
        temp = pro.getProperty("kafka.fetch.size");
        if(!StringUtils.isBlank(temp)) {
            fetchSize =Integer.valueOf(temp);
        }
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
