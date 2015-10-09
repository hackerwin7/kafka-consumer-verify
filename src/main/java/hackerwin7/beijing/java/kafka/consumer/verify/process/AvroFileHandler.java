package hackerwin7.beijing.java.kafka.consumer.verify.process;

import hackerwin7.beijing.java.kafka.consumer.verify.consume.AvroConsume;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by hp on 5/26/15.
 */
public class AvroFileHandler {
    private static String zks = null;
    private static String topic = null;
    private static int partition = 0;
    private static long offset = 0;
    private static String filterDb = null;
    private static String filterTb = null;
    private static String priKey = null;
    private static String priVals = null;
    private static String filterKv = null;
    private static String filterMid = null;
    private static String filterIp = null;
    private static String filterPar = null;
    private static String filterOff = null;
    private static String filterEndOff = null;
    private static String filterForce = null;
    private static String filterForceNum = null;
    private static long endOffset = Long.MAX_VALUE;

    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure(file2in("log4j.properties", "config.log4j"));
        initFile();
        AvroConsume consume = new AvroConsume();
        consume.setDbname(filterDb);
        consume.setTbname(filterTb);
        consume.setpKey(priKey);
        consume.setpVals(priVals);
        consume.setKvFilter(filterKv);
        consume.setMidFilter(filterMid);
        consume.setIpFilter(filterIp);
        consume.setPartitionFilter(filterPar);
        consume.setOffsetFilter(filterOff);
        consume.setEndOffsetFilter(filterEndOff);
        consume.setForceInput(filterForce);
        consume.setForceNum(filterForceNum);
        consume.startMulty(zks, topic);
    }

    private static void initFile() throws Exception {
        InputStream is = file2in("avro-config.properties", "config.conf");
        Properties pro = new Properties();
        pro.load(is);
        zks = pro.getProperty("kafka.zookeeper");
        topic = pro.getProperty("kafka.topic");
        filterPar = pro.getProperty("kafka.partition");
        filterOff = pro.getProperty("kafka.offset").trim();
        filterDb = pro.getProperty("filter.dbname");
        filterTb = pro.getProperty("filter.tbname");
        priKey = pro.getProperty("primary.key");
        priVals = pro.getProperty("primary.values");
        filterKv = pro.getProperty("filter.kv");
        filterMid = pro.getProperty("filter.mid");
        filterIp = pro.getProperty("filter.ip");
        filterEndOff = pro.getProperty("kafka.offset.end");
        filterForce = pro.getProperty("force.filter");
        filterForceNum = pro.getProperty("force.num");
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
