package hackerwin7.beijing.java.kafka.consumer.verify.consume;

import hackerwin7.beijing.java.kafka.consumer.verify.driver.KafkaData;
import hackerwin7.beijing.java.kafka.consumer.verify.driver.KafkaSimpleConsumer;
import hackerwin7.beijing.java.kafka.consumer.verify.protocol.consume.ConsumeData;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by hp on 9/23/15.
 */
public class TypeConsume {

    /*logger*/
    private static final Logger logger = Logger.getLogger(TypeConsume.class);

    /*data*/
    private String zks = null;
    private String topic = null;

    //multiple input
    private List<Integer> partitions = new ArrayList<Integer>();
    private List<Long> offsets = new ArrayList<Long>();
    private Map<Integer, Long> endOffsets = new HashMap<Integer, Long>();
    private Set<String> dbnames = new HashSet<String>();
    private Set<String> tbnames = new HashSet<String>();
    private Map<String, Set<String>> kvs = new HashMap<String, Set<String>>();// one key : multivalue
    private Set<Long> mids = new HashSet<Long>();
    private Set<String> ips = new HashSet<String>();
    private int fnum = 0;
    private String consumeType = null;

    /*controller*/
    private boolean running = true;

    public void setZks(String zks) throws Exception {
        if(!StringUtils.isBlank(zks)) {
            this.zks = zks;
        }
    }

    public void setTopic(String topic) throws Exception {
        if(!StringUtils.isBlank(topic)) {
            this.topic = topic;
        }
    }

    public void setPartitions(String partitions) throws Exception {
        if(!StringUtils.isBlank(partitions)) {
            String[] partArr = StringUtils.split(partitions, ",");
            for(String part : partArr) {
                this.partitions.add(Integer.valueOf(part));
            }
        }
    }

    public void setOffsets(String offsets) throws Exception {
        if(!StringUtils.isBlank(offsets)) {
            String[] offsetArr = StringUtils.split(offsets, ",");
            for (String offset : offsetArr) {
                this.offsets.add(Long.valueOf(offset));
            }
        }
    }

    public void setEndOffsets(String endOffsets) throws Exception {
        if(partitions.size() > 0) {
            for(int i = 0; i <= partitions.size() - 1; i++) {
                this.endOffsets.put(partitions.get(i), Long.MAX_VALUE);
            }
        }
        if(!StringUtils.isBlank(endOffsets)) {
            String[] endArr = StringUtils.split(endOffsets, ",");
            int i = 0;
            for(String end : endArr) {
                this.endOffsets.put(partitions.get(i), Long.valueOf(end));
                i++;
            }
        }
    }

    public void setDbnames(String dbnamesStr) throws Exception {
        if(!StringUtils.isBlank(dbnamesStr)) {
            String[] dbArr = StringUtils.split(dbnamesStr, ",");
            for(String db : dbArr) {
                dbnames.add(db);
            }
        }
    }

    public void setTbnames(String tbStr) throws Exception {
        if(!StringUtils.isBlank(tbStr)) {
            String[] tbArr = StringUtils.split(tbStr, ",");
            for(String tb: tbArr) {
                tbnames.add(tb);
            }
        }
    }

    public void setKvs(String kvsStr) throws Exception {
        if(!StringUtils.isBlank(kvsStr)) {
            String[] kvArr = StringUtils.split(kvsStr, ",");
            for(String kvStr : kvArr) {
                String[] kv = StringUtils.split(kvStr, ":");
                if(kvs.get(kv[0]) == null) {
                    Set<String> vals = new HashSet<String>();
                    vals.add(kv[1]);
                    kvs.put(kv[0], vals);
                } else {
                    kvs.get(kv[0]).add(kv[1]);
                }
            }
        }
    }

    public void setMids(String midsStr) throws Exception {
        if(!StringUtils.isBlank(midsStr)) {
            String[] midArr = StringUtils.split(midsStr, ",");
            for(String mid : midArr) {
                mids.add(Long.valueOf(mid));
            }
        }
    }

    public void setIps(String ipsStr) throws Exception {
        if(!StringUtils.isBlank(ipsStr)) {
            String[] ipArr = StringUtils.split(ipsStr, ",");
            for(String ip : ipArr) {
                ips.add(ip);
            }
        }
    }

    public void setFnum(String fnumStr) throws Exception {
        if(!StringUtils.isBlank(fnumStr)) {
            fnum = Integer.valueOf(fnumStr);
        }
    }

    public void setConsumeType(String consumeTypeStr) throws Exception {
        if(!StringUtils.isBlank(consumeTypeStr)) {
            consumeType = consumeTypeStr;
        } else {
            logger.error("consume type is error !!!");
            throw new Exception("consumer type could not be null");
        }
    }

    /**
     * start multiple consumer
     * @throws Exception
     */
    public void startMulty() throws Exception {
        String ss[] = zks.split("/");
        String zkServers, zkRoot;
        if(ss.length == 2) {
            zkServers = ss[0];
            zkRoot = "/" + ss[1];
        } else {
            zkServers = ss[0];
            zkRoot = "/";
        }
        BlockingQueue<KafkaData> queue = new ArrayBlockingQueue<KafkaData>(10000);
        List<KafkaSimpleConsumer> consumers = new ArrayList<KafkaSimpleConsumer>();
        for(int i = 0; i <= partitions.size() - 1; i++) {
            int partition = partitions.get(i);
            long offset = Long.MAX_VALUE;
            if( i <= (offsets.size() - 1)) {
                offset = offsets.get(i);
            }
            long endOffset = Long.MAX_VALUE;
            if(i <= (endOffsets.size() - 1)) {
                endOffset = endOffsets.get(i);
            }
            KafkaSimpleConsumer consumer = new KafkaSimpleConsumer(queue, zkServers, zkRoot);
            consumer.start(topic, partition, offset, endOffset);
            consumers.add(consumer);
        }
        while (running) {
            run(queue, consumers);
            logger.info("queue size = " + queue.size() + ", sleeping 2000 ms......");
            Thread.sleep(2000);
            if(queue.isEmpty() && getAllEnd(consumers)) {// it is really system exit symbol, ensure the simple consumer is stop and the queue is empty
                logger.info("reached end offset, exit the consume queue process ...... ");
                running = false;
            }
        }
    }

    /**
     * parse the consume data
     * @param queue
     * @param consumers
     * @throws Exception
     */
    private void run(BlockingQueue<KafkaData> queue, List<KafkaSimpleConsumer> consumers) throws Exception {
        long readNum = 0;
        while (!queue.isEmpty()) {
            readNum++;
            KafkaData data = queue.take();
            ConsumeData cdata = ConsumeData.parseFrom(consumeType, data);
            if(cdata == null) {
                continue;
            }
            //show internal header
            if(fnum > 0) {
                if(readNum % fnum == 0) {
                    logger.info(fnum + " ~~~~~~~~~~~~ internal : topic = " + cdata.getTopic() + ", partition = " + cdata.getPartitionNum() + ", offset = " + cdata.getOffset() + ", ip = " + cdata.getIp() +
                            " db = " + cdata.getDbname() + ", tb = " + cdata.getTbname() + ", timestamp = " + cdata.getTimestamp());
                }
            }
            if(dbnames.size() > 0) {
                if(!dbnames.contains(cdata.getDbname())) {
                    continue;
                }
            }
            if(tbnames.size() > 0) {
                if(!tbnames.contains(cdata.getTbname())) {
                    continue;
                }
            }
            if(mids.size() > 0 && !mids.contains(cdata.getMid())) {
                continue;
            }
            if(ips.size() > 0 && !ips.contains(cdata.getIp())) {
                continue;
            }
            if(kvs.size() > 0) {
                int occur = 0;
                //src
                for(Map.Entry<String, String> entry : cdata.getSrc().entrySet()) {
                    String key = entry.getKey();
                    String val = entry.getValue();
                    if(kvs.containsKey(key) && kvs.get(key).contains(val)) {
                        occur++;
                    }
                }
                for(Map.Entry<String, String> entry : cdata.getCur().entrySet()) {
                    String key = entry.getKey();
                    String val = entry.getValue();
                    if(kvs.containsKey(key) && kvs.get(key).contains(val)) {
                        occur++;
                    }
                }
                if(occur == 0) {
                    continue;
                }
            }
            //show data
            logger.info("----------------------------------------------------------------------------------------");
            logger.info(cdata.getHeader());
            logger.info(cdata.getData());
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
}
