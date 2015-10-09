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
        consume.startMulty();
    }

    private static void initFile() throws Exception {
        InputStream is = file2in("consume.properties", "config.conf");
        Properties pro = new Properties();
        pro.load(is);
        zks = pro.getProperty("kafka.zookeeper");
        topic = pro.getProperty("kafka.topic");
        partitions = pro.getProperty("kafka.partitions");
        offsets = pro.getProperty("kafka.offsets");
        endOffsets = pro.getProperty("kafka.offsets.end");
        dbnames = pro.getProperty("filter.dbnames");
        tbname = pro.getProperty("filter.tbnames");
        kvs = pro.getProperty("filter.kvs");
        mids = pro.getProperty("filter.mids");
        ips = pro.getProperty("filter.ips");
        filterNum = pro.getProperty("filter.num");
        consumeType = pro.getProperty("consume.type");
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
