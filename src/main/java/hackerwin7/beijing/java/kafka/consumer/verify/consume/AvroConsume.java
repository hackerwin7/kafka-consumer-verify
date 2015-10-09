package hackerwin7.beijing.java.kafka.consumer.verify.consume;

import hackerwin7.beijing.java.kafka.consumer.verify.driver.KafkaData;
import hackerwin7.beijing.java.kafka.consumer.verify.driver.KafkaSimpleConsumer;
import hackerwin7.beijing.java.kafka.consumer.verify.protocol.avro.EntryAvroUtils;
import hackerwin7.beijing.java.kafka.consumer.verify.protocol.avro.EventEntryAvro;
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
 * Created by hp on 5/22/15.
 */
public class AvroConsume {
    private Logger logger = Logger.getLogger(AvroConsume.class);

    private KafkaSimpleConsumer kafkaConsumer = null;

    private Set<String> ids = new HashSet<String>();

    private boolean running = true;

    private Map<String, String> kvFilter = null;

    private Set<Long> midFilter = null;

    private Set<String> ipFilter = null;

    private List<Integer> partitionFilter = null;

    private List<Long> offsetFilter = null;

    private List<Long> endOffsetFilter = null;

    private Map<Integer, Long> startOffsetMap = null;

    private Map<Integer, Long> endOffsetMap = null;// key: partiton , value: endOffset

    private boolean forceInput = false;

    private long forceNum = 10000;

    private Set<String> dbname = null;
    private Set<String> tbname = null;
    private String pKey = null;
    private String pVals = null;



    private long endOffset = Long.MAX_VALUE;

    /**
     * init the filter
     * @throws Exception
     */
    private void init() throws Exception {
        ids.clear();
        if(!isNULL(pVals)) {
            String[] ss = pVals.split(",");
            for(String s : ss) {
                ids.add(s);
            }
        }
    }

    /**
     * force internal number
     * @param numStr
     * @throws Exception
     */
    public void setForceNum(String numStr) throws Exception {
        if(!StringUtils.isBlank(numStr)) {
            forceNum = Long.valueOf(numStr);
        }
    }

    /**
     * force input
     * @param forceInputStr
     * @throws Exception
     */
    public void setForceInput(String forceInputStr) throws Exception {
        if(!StringUtils.isBlank(forceInputStr)) {
            forceInput = true;
        }
    }

    /**
     * set end offset filter
     * @param endOffStr
     * @throws Exception
     */
    public void setEndOffsetFilter(String endOffStr) throws Exception {
        if(!StringUtils.isBlank(endOffStr)) {
            endOffsetFilter = new ArrayList<Long>();
            String[] endArr = endOffStr.split(",");
            int i  = 0;
            for(String end : endArr) {
                Long offset = new Long(end);
                endOffsetFilter.add(offset);
                int partition = partitionFilter.get(i);
                endOffsetMap.put(partition, offset);
                i++;
            }
        }
    }

    /**
     * set offset filter string
     * @param offStr
     * @throws Exception
     */
    public void setOffsetFilter(String offStr) throws Exception {
        if(!StringUtils.isBlank(offStr)) {
            offsetFilter = new ArrayList<Long>();
            String[] offArr = offStr.split(",");
            int i = 0;
            for(String off : offArr) {
                Long offset = new Long(off);
                offsetFilter.add(offset);
                int paritition = partitionFilter.get(i);
                startOffsetMap.put(paritition, offset);
                i++;
            }
        }
    }

    /**
     * set partition string
     * @param pStr
     * @throws Exception
     */
    public void setPartitionFilter(String pStr) throws Exception {
        if(!StringUtils.isBlank(pStr)) {
            partitionFilter = new ArrayList<Integer>();
            startOffsetMap = new HashMap<Integer, Long>();
            endOffsetMap = new HashMap<Integer, Long>();
            String[] pArr = pStr.split(",");
            for(String p : pArr) {
                int partition = Integer.valueOf(p);
                partitionFilter.add(partition);
                startOffsetMap.put(partition, 0L);
                endOffsetMap.put(partition, Long.MAX_VALUE);
            }
        }
    }

    /**
     * set ip filter
     * @param ips
     * @throws Exception
     */
    public void setIpFilter(String ips) throws Exception {
        if(!StringUtils.isBlank(ips)) {
            ipFilter = new HashSet<String>();
            String[] ipArr = ips.split(",");
            for(String ip : ipArr) {
                ipFilter.add(ip);
            }
        }
    }

    /**
     * set mid filter
     * @param mids, string
     * @throws Exception
     */
    public void setMidFilter(String mids) throws Exception {
        if(!StringUtils.isBlank(mids)) {
            midFilter = new HashSet<Long>();
            String[] midsArr = mids.split(",");
            for(String midStr : midsArr) {
                long mid = Long.valueOf(midStr);
                midFilter.add(mid);
            }
        }
    }

    /**
     * set filter string
     * @param kvs
     * @throws Exception
     */
    public void setKvFilter(String kvs) throws Exception {
        if(!StringUtils.isBlank(kvs)) {
            kvFilter = new HashMap<String, String>();
            String[] kvsArr = kvs.split(",");
            for(String kv : kvsArr) {
                String[] kvElem = kv.split(":");
                String key = kvElem[0];
                String val = kvElem[1];
                kvFilter.put(key, val);
            }
        }
    }

    /**
     * start the job
     * @param zkStr
     * @param topic
     * @throws Exception
     */
    public void startMulty(String zkStr, String topic) throws Exception {
        init();
        String ss[] = zkStr.split("/");
        String zkServers, zkRoot;
        if(ss.length == 2) {
            zkServers = ss[0];
            zkRoot = "/" + ss[1];
        } else {
            zkServers = ss[0];
            zkRoot = "/";
        }
        BlockingQueue<KafkaData> queue = new LinkedBlockingQueue<KafkaData>(10000);
        // start multiple kafka
        List<KafkaSimpleConsumer> consumers = new ArrayList<KafkaSimpleConsumer>();
        for(int i = 0; i <= partitionFilter.size() - 1; i++) {
            int partition = partitionFilter.get(i);
            long offset = offsetFilter.get(i);
            long endOffset = endOffsetMap.get(partition);
            kafkaConsumer = new KafkaSimpleConsumer(queue, zkServers, zkRoot);
            kafkaConsumer.start(topic, partition, offset, endOffset);
            consumers.add(kafkaConsumer);
        }
        while (running) {
            run(queue, consumers);
        }
    }

    /**
     * get all thread is end or not
     * @param consumers
     * @return boolean
     * @throws Exception
     */
    private boolean getAllEnd(List<KafkaSimpleConsumer> consumers) throws Exception {
        for(KafkaSimpleConsumer consumer : consumers) {
            if(consumer.isRunning()) {
                return false;
            }
        }
        return true;
    }

    /**
     * simple consumers
     * @param queue
     * @param consumers
     * @throws Exception
     */
    private void run(BlockingQueue<KafkaData> queue, List<KafkaSimpleConsumer> consumers) throws Exception {
        long cnt = 0;
        tag:
        while (!queue.isEmpty()) {
            int isPrePrint = 0;// equals kv number
            int isCurPrint = 0;
            boolean isMid = false;
            boolean isIp = false;
            KafkaData data = queue.take();
            if(getAllEnd(consumers)) {
                logger.info("reached end offset, exit the process ......");
                running = false;
                break;
            }
            byte[] value = data.getValue();
            EventEntryAvro entry = EntryAvroUtils.bytes2Avro(value);
            if(entry == null) continue;
            String getDb = entry.getDb().toString();
            String getTb = entry.getTab().toString();
            if(dbname != null && dbname.size() > 0) {
                if(!dbname.contains(getDb)) {
                    continue;
                }
            }
            if(tbname != null && tbname.size() > 0) {
                if(!tbname.contains(getTb)) {
                    continue;
                }
            }
            if(midFilter != null && midFilter.contains(entry.getMid())) {
                isMid = true;
            }
            StringBuilder sb = new StringBuilder();
            //ip and tracker time
            if(entry.getCus() != null) {
                sb.append("=========> cus : {");
                for (Map.Entry mEntry : entry.getCus().entrySet()) {
                    String mKey = mEntry.getKey().toString();
                    String temp = null;
                    if (mEntry.getValue() != null) {
                        temp = mEntry.getValue().toString();
                    }
                    String mValue = temp;
                    if(ipFilter != null && ipFilter.contains(mValue)) {
                        isIp = true;
                    }
                    sb.append(mKey + ":" + mValue + ",");
                }
                sb.append("}");
                sb.append("\n");
            }
            //source row data
            if(entry.getSrc() != null) {
                sb.append("=========> src : {");
                for (Map.Entry mEntry : entry.getSrc().entrySet()) {
                    String mKey = mEntry.getKey().toString();
                    String temp = null;
                    if (mEntry.getValue() != null) {
                        temp = mEntry.getValue().toString();
                    }
                    String mValue = temp;
                    if(kvFilter != null && kvFilter.containsKey(mKey) && kvFilter.get(mKey).equals(mValue)) {
                        isPrePrint++;
                    }
                    sb.append(mKey + ":" + mValue + ",");
                }
                sb.append("}");
                sb.append("\n");
            }
            //current row data
            if(entry.getCur() != null) {
                sb.append("=========> cur : {");
                int keyCnt = 0;
                for (Map.Entry mEntry : entry.getCur().entrySet()) {
                    String mKey = mEntry.getKey().toString();
                    String temp = null;
                    if (mEntry.getValue() != null) {
                        temp = mEntry.getValue().toString();
                    }
                    String mValue = temp;
                    if(kvFilter != null && kvFilter.containsKey(mKey) && kvFilter.get(mKey).equals(mValue)) {
                        isCurPrint++;
                    }
                    sb.append(mKey + ":" + mValue + ",");
                    if (!isNULL(pKey)) {
                        if (mKey.equals(pKey)) {
                            if (ids.size() > 0 && !ids.contains(mValue)) {
                                continue tag;
                            }
                            keyCnt++;
                        }
                    }
                }
                if (!isNULL(pKey) && keyCnt <= 0) {
                    continue;
                }
                sb.append("}");
            }
            cnt++;
            if(!forceInput) {
                logger.info("---------------------------------------------------------------------------------------------------------------");
                logger.info("avro entry dbName = " + entry.getDb() + ", tbName = " + entry.getTab() + ", type = " + entry.getOpt() + ", partition = " + data.getPartition() + ", offset = " + data.getOffset() + ", mid = " + entry.getMid()
                        + ", parser time = " + entry.getTs());
            } else {
                if(cnt % forceNum == 0) {
                    logger.info(forceNum + " :  -----------------------------------------------------------------------------------------------------------");
                    logger.info("avro entry dbName = " + entry.getDb() + ", tbName = " + entry.getTab() + ", type = " + entry.getOpt() + ", partition = " + data.getPartition() + ", offset = " + data.getOffset() + ", mid = " + entry.getMid()
                            + ", parser time = " + entry.getTs());
                    cnt = 0;
                }
            }
            if(kvFilter == null || kvFilter.size() == 0) {
                if(midFilter != null && midFilter.size() > 0) {
                    if(isMid) {
                        if(ipFilter != null && ipFilter.size() > 0) {
                            if(isIp) {
                                if(forceInput) {
                                    logger.info("---------------------------------------------------------------------------------------------------------------");
                                    logger.info("avro entry dbName = " + entry.getDb() + ", tbName = " + entry.getTab() + ", type = " + entry.getOpt() + ", partition = " + data.getPartition() + ", offset = " + data.getOffset() + ", mid = " + entry.getMid()
                                            + ", parser time = " + entry.getTs());
                                }
                                logger.info("avro detail data : \n" + sb.toString());
                            }
                        } else {
                            if(forceInput) {
                                logger.info("---------------------------------------------------------------------------------------------------------------");
                                logger.info("avro entry dbName = " + entry.getDb() + ", tbName = " + entry.getTab() + ", type = " + entry.getOpt() + ", partition = " + data.getPartition() + ", offset = " + data.getOffset() + ", mid = " + entry.getMid()
                                        + ", parser time = " + entry.getTs());
                            }
                            logger.info("avro detail data : \n" + sb.toString());
                        }
                    }
                } else {
                    if(ipFilter != null && ipFilter.size() > 0) {
                        if(isIp) {
                            if(forceInput) {
                                logger.info("---------------------------------------------------------------------------------------------------------------");
                                logger.info("avro entry dbName = " + entry.getDb() + ", tbName = " + entry.getTab() + ", type = " + entry.getOpt() + ", partition = " + data.getPartition() + ", offset = " + data.getOffset() + ", mid = " + entry.getMid()
                                        + ", parser time = " + entry.getTs());
                            }
                            logger.info("avro detail data : \n" + sb.toString());
                        }
                    } else {
                        if(forceInput) {
                            logger.info("---------------------------------------------------------------------------------------------------------------");
                            logger.info("avro entry dbName = " + entry.getDb() + ", tbName = " + entry.getTab() + ", type = " + entry.getOpt() + ", partition = " + data.getPartition() + ", offset = " + data.getOffset() + ", mid = " + entry.getMid()
                                    + ", parser time = " + entry.getTs());
                        }
                        logger.info("avro detail data : \n" + sb.toString());
                    }
                }
            } else {
                if (isPrePrint > 0 || isCurPrint > 0) {
                    if (midFilter != null && midFilter.size() > 0) {
                        if (isMid) {
                            if (ipFilter != null && ipFilter.size() > 0) {
                                if (isIp) {
                                    if(forceInput) {
                                        logger.info("---------------------------------------------------------------------------------------------------------------");
                                        logger.info("avro entry dbName = " + entry.getDb() + ", tbName = " + entry.getTab() + ", type = " + entry.getOpt() + ", partition = " + data.getPartition() + ", offset = " + data.getOffset() + ", mid = " + entry.getMid()
                                                + ", parser time = " + entry.getTs());
                                    }
                                    logger.info("avro detail data : \n" + sb.toString());
                                }
                            } else {
                                if(forceInput) {
                                    logger.info("---------------------------------------------------------------------------------------------------------------");
                                    logger.info("avro entry dbName = " + entry.getDb() + ", tbName = " + entry.getTab() + ", type = " + entry.getOpt() + ", partition = " + data.getPartition() + ", offset = " + data.getOffset() + ", mid = " + entry.getMid()
                                            + ", parser time = " + entry.getTs());
                                }
                                logger.info("avro detail data : \n" + sb.toString());
                            }
                        }
                    } else {
                        if (ipFilter != null && ipFilter.size() > 0) {
                            if (isIp) {
                                if(forceInput) {
                                    logger.info("---------------------------------------------------------------------------------------------------------------");
                                    logger.info("avro entry dbName = " + entry.getDb() + ", tbName = " + entry.getTab() + ", type = " + entry.getOpt() + ", partition = " + data.getPartition() + ", offset = " + data.getOffset() + ", mid = " + entry.getMid()
                                            + ", parser time = " + entry.getTs());
                                }
                                logger.info("avro detail data : \n" + sb.toString());
                            }
                        } else {
                            if(forceInput) {
                                logger.info("---------------------------------------------------------------------------------------------------------------");
                                logger.info("avro entry dbName = " + entry.getDb() + ", tbName = " + entry.getTab() + ", type = " + entry.getOpt() + ", partition = " + data.getPartition() + ", offset = " + data.getOffset() + ", mid = " + entry.getMid()
                                        + ", parser time = " + entry.getTs());
                            }
                            logger.info("avro detail data : \n" + sb.toString());
                        }
                    }
                }
            }
        }
    }

    /**
     * start the job
     * @param zkStr
     * @param topic
     * @param partition
     * @param offset
     * @throws Exception
     */
    public void start(String zkStr, String topic, int partition, long offset) throws Exception {
        init();
        String ss[] = zkStr.split("/");
        String zkServers, zkRoot;
        if(ss.length == 2) {
            zkServers = ss[0];
            zkRoot = "/" + ss[1];
        } else {
            zkServers = ss[0];
            zkRoot = "/";
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
     * consume the data
     * @param queue
     * @throws Exception
     */
    private void run(BlockingQueue<KafkaData> queue) throws Exception {
        tag:
        while (!queue.isEmpty()) {
            int isPrePrint = 0;// equals kv number
            int isCurPrint = 0;
            boolean isMid = false;
            boolean isIp = false;
            KafkaData data = queue.take();
            if(data.getOffset() > endOffset) {
                logger.info("reached end offset, exit the process ......");
                running = false;
                break;
            }
            byte[] value = data.getValue();
            EventEntryAvro entry = EntryAvroUtils.bytes2Avro(value);
            if(entry == null) continue;
            String getDb = entry.getDb().toString();
            String getTb = entry.getTab().toString();
            if(dbname != null && dbname.size() > 0) {
                if(!dbname.contains(getDb)) {
                    continue;
                }
            }
            if(tbname != null && tbname.size() > 0) {
                if(!tbname.contains(getTb)) {
                    continue;
                }
            }
            if(midFilter != null && midFilter.contains(entry.getMid())) {
                isMid = true;
            }
            StringBuilder sb = new StringBuilder();
            //ip and tracker time
            if(entry.getCus() != null) {
                sb.append("=========> cus : {");
                for (Map.Entry mEntry : entry.getCus().entrySet()) {
                    String mKey = mEntry.getKey().toString();
                    String temp = null;
                    if (mEntry.getValue() != null) {
                        temp = mEntry.getValue().toString();
                    }
                    String mValue = temp;
                    if(ipFilter != null && ipFilter.contains(mValue)) {
                        isIp = true;
                    }
                    sb.append(mKey + ":" + mValue + ",");
                }
                sb.append("}");
                sb.append("\n");
            }
            //source row data
            if(entry.getSrc() != null) {
                sb.append("=========> src : {");
                for (Map.Entry mEntry : entry.getSrc().entrySet()) {
                    String mKey = mEntry.getKey().toString();
                    String temp = null;
                    if (mEntry.getValue() != null) {
                        temp = mEntry.getValue().toString();
                    }
                    String mValue = temp;
                    if(kvFilter != null && kvFilter.containsKey(mKey) && kvFilter.get(mKey).equals(mValue)) {
                        isPrePrint++;
                    }
                    sb.append(mKey + ":" + mValue + ",");
                }
                sb.append("}");
                sb.append("\n");
            }
            //current row data
            if(entry.getCur() != null) {
                sb.append("=========> cur : {");
                int keyCnt = 0;
                for (Map.Entry mEntry : entry.getCur().entrySet()) {
                    String mKey = mEntry.getKey().toString();
                    String temp = null;
                    if (mEntry.getValue() != null) {
                        temp = mEntry.getValue().toString();
                    }
                    String mValue = temp;
                    if(kvFilter != null && kvFilter.containsKey(mKey) && kvFilter.get(mKey).equals(mValue)) {
                        isCurPrint++;
                    }
                    sb.append(mKey + ":" + mValue + ",");
                    if (!isNULL(pKey)) {
                        if (mKey.equals(pKey)) {
                            if (ids.size() > 0 && !ids.contains(mValue)) {
                                continue tag;
                            }
                            keyCnt++;
                        }
                    }
                }
                if (!isNULL(pKey) && keyCnt <= 0) {
                    continue;
                }
                sb.append("}");
            }
            logger.info("---------------------------------------------------------------------------------------------------------------");
            logger.info("avro entry dbName = " + entry.getDb() + ", tbName = " + entry.getTab() + ", type = " + entry.getOpt() + ", offset = " + data.getOffset() + ", mid = " + entry.getMid()
                    + ", parser time = " + entry.getTs());
            if(kvFilter == null || kvFilter.size() == 0) {
                if(midFilter != null && midFilter.size() > 0) {
                    if(isMid) {
                        if(ipFilter != null && ipFilter.size() > 0) {
                            if(isIp) {
                                logger.info("avro detail data : \n" + sb.toString());
                            }
                        } else {
                            logger.info("avro detail data : \n" + sb.toString());
                        }
                    }
                } else {
                    if(ipFilter != null && ipFilter.size() > 0) {
                        if(isIp) {
                            logger.info("avro detail data : \n" + sb.toString());
                        }
                    } else {
                        logger.info("avro detail data : \n" + sb.toString());
                    }
                }
            } else {
                if (isPrePrint > 0 || isCurPrint > 0) {
                    if (midFilter != null && midFilter.size() > 0) {
                        if (isMid) {
                            if (ipFilter != null && ipFilter.size() > 0) {
                                if (isIp) {
                                    logger.info("avro detail data : \n" + sb.toString());
                                }
                            } else {
                                logger.info("avro detail data : \n" + sb.toString());
                            }
                        }
                    } else {
                        if (ipFilter != null && ipFilter.size() > 0) {
                            if (isIp) {
                                logger.info("avro detail data : \n" + sb.toString());
                            }
                        } else {
                            logger.info("avro detail data : \n" + sb.toString());
                        }
                    }
                }
            }
        }
    }

    /**
     * setter
     * @param dbname
     */
    public void setDbname(String dbname) {
        if(!StringUtils.isBlank(dbname)) {
            this.dbname = new HashSet<String>();
            String[] dbArr = dbname.split(",");
            for(String db : dbArr) {
                this.dbname.add(db);
            }
        }
    }

    /**
     * setter
     * @param tbname
     */
    public void setTbname(String tbname) {
        if(!StringUtils.isBlank(tbname)) {
            this.tbname = new HashSet<String>();
            String[] tbArr = tbname.split(",");
            for(String tb : tbArr) {
                this.tbname.add(tb);
            }
        }
    }

    /**
     * setter
     * @param pVals
     */
    public void setpVals(String pVals) {
        this.pVals = pVals;
    }

    /**
     * setter
     * @param pKey
     */
    public void setpKey(String pKey) {
        this.pKey = pKey;
    }

    /**
     * setter
     * @param endOffset
     */
    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    /**
     * str is null or "" str trim
     * @param str
     * @return
     * @throws Exception
     */
    private boolean isNULL(String str) throws Exception {
        if(str == null) {
            return true;
        }
        str = str.trim();
        if(str.equals("")) {
            return true;
        }
        return false;
    }
}
