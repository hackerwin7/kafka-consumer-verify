package hackerwin7.beijing.java.kafka.consumer.verify.consume;

import hackerwin7.beijing.java.kafka.consumer.verify.driver.KafkaData;
import hackerwin7.beijing.java.kafka.consumer.verify.driver.KafkaSimpleConsumer;
import hackerwin7.beijing.java.kafka.consumer.verify.protocol.protobuf.All;
import hackerwin7.beijing.java.kafka.consumer.verify.protocol.protobuf.Common;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * consume the data from the kafka
 * Created by hp on 8/6/15.
 */
public class LogProtobufConsume {

    private static final Logger logger = Logger.getLogger(LogProtobufConsume.class);

    private KafkaSimpleConsumer kafkaConsumer = null;

    private Set<String> ids = new HashSet<String>();

    private boolean running = true;

    private long endOffset = Long.MAX_VALUE;

    /*filter string*/
    private String filterStr = null;
    private String filterStrData = null;

    /*constants*/
    public static String VALUE_NOTNULL = "NOTNULL";
    public static String VALUE_NULL = "NULL";
    public static String KEY_DATA_NUM = "dataNum";

    /*filter map*/
    private Map<String, String> fkv = new HashMap<String, String>();
    private Map<String, Boolean> fkvBool = new HashMap<String, Boolean>();
    private Map<String, String> fkvData = new HashMap<String, String>();
    private Map<String, Boolean> fkvBoolData = new HashMap<String, Boolean>();

    /*msg map save this, 0 is header , other are datas*/
    private List<Map<String, String>> mp = new ArrayList<Map<String, String>>();


    /*count for msg header and data*/
    private int headerNum = 0;
    private int dataNum = 0;

    /**
     * set filter string
     * @param str
     * @throws Exception
     */
    public void setFilterStr(String str) throws Exception {
        filterStr = str;
        fkv.clear();
        if(!StringUtils.isBlank(filterStr)) {
            String[] kvs = filterStr.split(",");
            for(String kv : kvs) {
                String[] elem = kv.split(":");
                String key = elem[0];
                String val = elem[1];
                fkv.put(key, val);
                fkvBool.put(key, false);//init the bool map
            }
        }
    }

    /**
     * set filter data
     * @throws Exception
     */
    public void setFilterStrData(String str) throws Exception {
        filterStrData = str;
        fkvData.clear();
        if(!StringUtils.isBlank(filterStrData)) {
            String[] kvs = filterStrData.split(",");
            for(String kv : kvs) {
                String[] elem = kv.split(":");
                String key = elem[0];
                String val = elem[1];
                fkvData.put(key, val);
                fkvBoolData.put(key, false);//init the bool map
            }
        }
    }


    /**
     * start the consumer job
     * @param zkStr, zookeeper connection string
     * @param topic, kafka topic
     * @param partition, kafka partition number
     * @param offset, kafka partition offset
     * @throws Exception
     */
    public void start(String zkStr, String topic, int partition, long offset) throws Exception {
        String[] ss = zkStr.split("/");
        String zkServers, zkRoot;
        if(ss.length == 2) {
            zkServers = ss[0];
            zkRoot = "/" + ss[1];
        } else {
            zkServers = ss[0];
            zkRoot = "/";
        }
        if(zkRoot.equals("/")) { // zkroot is "/" + "/brokers/ids" is error
            zkRoot = "";
        }
        BlockingQueue<KafkaData> queue = new LinkedBlockingQueue<KafkaData>(10000);
        kafkaConsumer = new KafkaSimpleConsumer(queue, zkServers, zkRoot);
        kafkaConsumer.start(topic, partition, offset, endOffset);
        while (running) {
            run(queue);
        }
        kafkaConsumer.stop();
    }

    /**
     * consume and parse the message
     * @param queue, message queue
     * @throws Exception
     */
    private void run(BlockingQueue<KafkaData> queue) throws Exception {
        tag:
        while (!queue.isEmpty()) {
            KafkaData data = queue.take();
            if(data.getOffset() > endOffset) {
                logger.info("reached end offset, exit the process ......");
                running = false;
                break;
            }
            //get the kafka message byte[]
            byte[] value = data.getValue();
            long offset = data.getOffset();
            //show value
            //logger.info("bytes:" + value);
            //parse the bytes to protobuf
            if(value != null && value.length > 0) {
                try {
                    All.all_msg msg = All.all_msg.parseFrom(value);
                    if (msg != null) {
                        /*put msg to map*/
                        putMap(msg);
                        checkValidHeader();
                        checkValidData();
                        if(isPrint()) {
                            logger.info("============================ msg :");
                            logger.info("offset : " + offset);
                            logger.info("data num : " + dataNum);
                            logger.info("data : " + msg);
                        }
                        clearMapList();
                    }
                } catch (Exception e)  {
                    logger.error("parse error!!!!!!!!!!!!!!! " + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * clear and init the map
     * @throws Exception
     */
    private void clearMapList() throws Exception {
        mp.clear();
        for (Map.Entry<String, String> entry : fkv.entrySet()) {
            fkvBool.put(entry.getKey(), false);
        }
        for (Map.Entry<String, String> entry : fkvData.entrySet()) {
            fkvBoolData.put(entry.getKey(), false);
        }
    }

    /**
     * put protobuf message to map
     * @param msg
     * @throws Exception
     */
    private void putMap(All.all_msg msg) throws Exception {
        headerNum = 0;
        dataNum = 0;
        /*field*/
        Map<String, String> fieldMap = new HashMap<String, String>();
        for(Common.pair kv : msg.getFieldList()) {
            String key = kv.getKey();
            String val = kv.getVal();
            fieldMap.put(key, val);
        }
        if(fieldMap.size() > 0) {
            /*mp put dataNum field*/
            fieldMap.put(KEY_DATA_NUM, String.valueOf(msg.getDataCount()));
            mp.add(fieldMap);
            headerNum = 1;
        }
        /*datas*/
        for(All.sub_msg data : msg.getDataList()) {
            Map<String, String> dataMap = new HashMap<String, String>();
            for(Common.pair kv : data.getFieldList()) {
                String key = kv.getKey();
                String val = kv.getVal();
                dataMap.put(key, val);
            }
            if(dataMap.size() > 0) {
                /*mp put dataNum field*/
                dataMap.put(KEY_DATA_NUM, String.valueOf(msg.getDataCount()));
                mp.add(dataMap);
                dataNum++;
            }
        }
    }

    /**
     * check the filter valid
     * @throws Exception
     */
    private void checkValidHeader() throws Exception {
        /*check filter key value*/
        for(Map.Entry<String, String> entry : fkv.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();

            String vkey = null;
            String vval = null;

            /*find message key value*/
            if(headerNum > 0) {

                Map<String, String> kvmap = mp.get(0);

                if (kvmap.containsKey(key)) {//find the key
                    vkey = key;
                    vval = kvmap.get(key);
                }

                /*not exist*/
                if(vkey == null) {
                    fkvBool.put(key, false);
                    continue;
                }

                /* exist the key val then  check for 3 mode*/
                if(val.equals(VALUE_NOTNULL) || val.equals(VALUE_NULL)) { // check null or not null
                    if(val.equals(VALUE_NULL)) {
                        if(StringUtils.isBlank(vval)) {
                            fkvBool.put(key, true);
                        } else {
                            fkvBool.put(key, false);
                        }
                    } else {
                        if(StringUtils.isBlank(vval)) {
                            fkvBool.put(key, false);
                        } else {
                            fkvBool.put(key, true);
                        }
                    }
                } else if(key.equals(KEY_DATA_NUM)) {
                    if(val.equals("0")) {// dataNum == 0
                        if(dataNum == 0) {
                            fkvBool.put(key, true);
                        } else {
                            fkvBool.put(key, false);
                        }
                    } else { // dataNum > 0
                        if(dataNum == 0) {
                            fkvBool.put(key, false);
                        } else {
                            fkvBool.put(key, true);
                        }
                    }
                } else { // val == vval
                    if (val.equals(vval)) {
                        fkvBool.put(key, true);
                    } else {
                        fkvBool.put(key, false);
                    }
                }
            }
        }
    }

    /**
     * check data field
     * @throws Exception
     */
    private void checkValidData() throws Exception {
        for(Map.Entry<String, String> entry : fkvData.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();

            /*find message key value*/

            for(int i = 1; i <= mp.size() - 1; i++) {

                String vkey = null;
                String vval = null;

                Map<String, String> kvmap = mp.get(i);

                if (kvmap.containsKey(key)) {//find the key
                    vkey = key;
                    vval = kvmap.get(key);
                }

                /*not exist*/
                if (vkey == null) {
                    fkvBoolData.put(key, false);
                    continue;
                }

                /* exist the key val then  check for 3 mode*/
                if (val.equals(VALUE_NOTNULL) || val.equals(VALUE_NULL)) { // check null or not null
                    if (val.equals(VALUE_NULL)) {
                        if (StringUtils.isBlank(vval)) {
                            fkvBoolData.put(key, true);
                        } else {
                            fkvBoolData.put(key, false);
                        }
                    } else {
                        if (StringUtils.isBlank(vval)) {
                            fkvBoolData.put(key, false);
                        } else {
                            fkvBoolData.put(key, true);
                        }
                    }
                } else if (key.equals(KEY_DATA_NUM)) {
                    if (val.equals("0")) {// dataNum == 0
                        if (dataNum == 0) {
                            fkvBoolData.put(key, true);
                        } else {
                            fkvBoolData.put(key, false);
                        }
                    } else { // dataNum > 0
                        if (dataNum == 0) {
                            fkvBoolData.put(key, false);
                        } else {
                            fkvBoolData.put(key, true);
                        }
                    }
                } else { // val == vval
                    if (val.equals(vval)) {
                        fkvBoolData.put(key, true);
                    } else {
                        fkvBoolData.put(key, false);
                    }
                }

                // if true,  this key is true so skip it
                if(fkvBoolData.get(key).equals(true)) {
                    break;
                }
            }
        }
    }

    /**
     * is or not printing this msg
     * @return boolean
     * @throws Exception
     */
    private boolean isPrint() throws Exception {
        for (Map.Entry<String, Boolean> entry : fkvBool.entrySet()) {
            if(entry.getValue().equals(false)) {
                return false;
            }
        }
        for (Map.Entry<String, Boolean> entry : fkvBoolData.entrySet()) {
            if(entry.getValue().equals(false)) {
                return false;
            }
        }
        return true;
    }
}
