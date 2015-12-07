package hackerwin7.beijing.java.kafka.consumer.verify.process;

import hackerwin7.beijing.java.kafka.consumer.verify.consume.TypeConsume;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 * Created by hp on 9/23/15.
 */
public class ConsumeHandler {

    /*if string contains any one it woud print it*/
    private static String zks = null;
    private static String topic = null;//single topic
    private static String partitions = null;// 0,1,2,3,4,5
    private static String offsets = null;// 100,101,102
    private static String endOffsets = null;//100,101,102
    private static String dbnames = null;//d1,d2,d3,d4
    private static String tbname = null;//t1,t2,t3,t4
    private static String kvs = null;//k1:v1,k2:v2,k3:v3
    private static String mids = null;//mid1,mid2,mid3
    private static String ips = null;//ip1,ip2,ip3
    private static String filterNum = null;//single number for internal 0 is print everyone
    private static String consumeType = null;//avro,protobuf,rowdataProtobuf
    private static String consumeStrategy = null;
    private static String appid = null;
    private static String token = null;

    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure(file2in("log4j.properties", "config.log4j"));
        initFile();
        TypeConsume consume = new TypeConsume();
        consume.setZks(zks);
        consume.setTopic(topic);
        consume.setPartitions(partitions);
        consume.setOffsets(offsets);
        consume.setEndOffsets(endOffsets);
        consume.setDbnames(dbnames);
        consume.setTbnames(tbname);
        consume.setKvs(kvs);
        consume.setMids(mids);
        consume.setIps(ips);
        consume.setFnum(filterNum);
        consume.setConsumeType(consumeType);
        consume.setConsumeStrategy(consumeStrategy);
        consume.setAppid(appid);
        consume.setToken(token);
        consume.start();
    }

    private static void initFile() throws Exception {
        InputStream is = file2in("consume.properties", "config.conf");
        Properties pro = new Properties();
        pro.load(is);
        zks = pro.getProperty("kafka.zookeeper").trim();
        topic = pro.getProperty("kafka.topic").trim();
        partitions = pro.getProperty("kafka.partitions").trim();
        offsets = pro.getProperty("kafka.offsets").trim();
        endOffsets = pro.getProperty("kafka.offsets.end").trim();
        dbnames = pro.getProperty("filter.dbnames").trim();
        tbname = pro.getProperty("filter.tbnames").trim();
        kvs = pro.getProperty("filter.kvs").trim();
        mids = pro.getProperty("filter.mids").trim();
        ips = pro.getProperty("filter.ips").trim();
        filterNum = pro.getProperty("filter.num").trim();
        consumeType = pro.getProperty("consume.data.type").trim();
        consumeStrategy = pro.getProperty("consume.strategy").trim();
        appid = pro.getProperty("kafka.appid").trim();
        token = pro.getProperty("kafka.token").trim();
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
