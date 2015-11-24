package hackerwin7.beijing.java.kafka.consumer.verify.driver;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Created by hp on 5/22/15.
 */
public class KafkaSimpleConsumer {
    private Logger logger = Logger.getLogger(KafkaSimpleConsumer.class);

    private BlockingQueue<KafkaData> queue = null;
    private List<String> brokerList = null;
    private Map<String, SimpleConsumer> consumerLib = null;
    private boolean running = true;

    private Thread thread = null;


    public static final int FETCH_SIZE = 1000000;

    private int consumerFetchSize = FETCH_SIZE;

    /**
     * constructor, it must be has itself queue
     * @param _queue
     */
    public KafkaSimpleConsumer(BlockingQueue<KafkaData> _queue, String zkServers, String zkRoot) throws Exception {
        queue = _queue;
        brokerList = new LinkedList<String>();
        consumerLib = new HashMap<String, SimpleConsumer>();
        loadZk(zkServers, zkRoot);
    }

    public KafkaSimpleConsumer(BlockingQueue<KafkaData> _queue, String zkServers, String zkRoot, int fetchSize) throws Exception {
        queue = _queue;
        brokerList = new LinkedList<String>();
        consumerLib = new HashMap<String, SimpleConsumer>();
        consumerFetchSize = fetchSize;
        loadZk(zkServers, zkRoot);
    }

    public void start(final String topic, final int partition, final long initOffset, final long endOffset) throws Exception {
        logger.info("starting kafka consumer = " + "[" + "topic : " + topic + ", partition : " + partition + ", startOffset : " + initOffset + ", endOffset : " + endOffset + "]");
        thread = new Thread(new Runnable() {
            public void run() {
                try {
                    dump(topic, partition, brokerList, initOffset, endOffset);
                } catch (Exception e) {
                    logger.error("simple consumer encounter error......", e);
                    running = false;
                }
            }
        });
        thread.start();
    }

    /**
     * get broker info by zookeeper
     * @param zks
     * @param zkr
     * @throws Exception
     */
    private void loadZk(String zks, String zkr) throws Exception {
        ZkExecutor zk = new ZkExecutor(zks);
        String brokerPath = zkr + "/brokers/ids";
        List<String> nameList = zk.getChildren(brokerPath);
        for(String name : nameList) {
            String info = zk.get(brokerPath + "/" + name);
            JSONObject jInfo = JSONObject.fromObject(info);
            String host = jInfo.getString("host");
            int port = jInfo.getInt("port");
            brokerList.add(host + ":" + port);
        }
        zk.close();//close the zk connection properly
    }

    /**
     * find the leader broker
     * @param brokers
     * @param topic
     * @param partition
     * @return
     * @throws Exception
     */
    private PartitionMetadata findLeader(List<String> brokers, String topic, int partition) throws Exception {
        PartitionMetadata metadata = null;
        loop:
        for(String info : brokers) {
            String[] ss = info.split(":");
            String broker = ss[0];
            int port = Integer.valueOf(ss[1]);
            SimpleConsumer consumer = null;
            try {
                consumer = createSimpleConsumer(broker, port, partition, topic, "clientTest");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                TopicMetadataResponse resp = consumer.send(req);
                List<TopicMetadata> metadataList = resp.topicsMetadata();
                for (TopicMetadata item : metadataList) {
                    for(PartitionMetadata part : item.partitionsMetadata()) {
                        if(part.partitionId() == partition) {
                            metadata = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error communicating with Broker [" + broker + "] to find Leader for [" + topic
                        + ", " + partition + "] Reason: " + e, e);
            }
        }
        if(metadata != null) {
            brokerList.clear();
            for (Broker broker : metadata.replicas()) {
                brokerList.add(broker.host() + ":" + broker.port());
            }
        }
        return metadata;
    }

    /**
     * find new leader
     * @param broker
     * @param port
     * @param topic
     * @param partition
     * @return
     * @throws Exception
     */
    private String findNewLeader(String broker, int port, String topic, int partition) throws Exception {
        for(int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(brokerList, topic, partition);
            if(metadata == null) {
                goToSleep = true;
            } else if(metadata.leader() == null) {
                goToSleep = true;
            } else if(broker.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                goToSleep = true;
            } else {
                return metadata.leader().host() + ":" + metadata.leader().port();
            }
            if(goToSleep) {
                Thread.sleep(1000);
            }
        }
        logger.error("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    /**
     * consumer create
     * @param broker
     * @param port
     * @param partition
     * @param topic
     * @param clientName
     * @return
     * @throws Exception
     */
    private SimpleConsumer createSimpleConsumer(String broker, int port, int partition, String topic, String clientName) throws Exception {
        String key = broker + port + partition + topic;
        if(!consumerLib.containsKey(key)) {
            SimpleConsumer consumer = new SimpleConsumer(broker, port, 100000, 64 * 1024, clientName);
            consumerLib.put(key, consumer);
        }
        return consumerLib.get(key);
    }

    /**
     * get earliest offset
     * @param consumer
     * @param topic
     * @param partition
     * @param clientName
     * @return
     * @throws Exception
     */
    private long getEarliestOffset(SimpleConsumer consumer, String topic, int partition, String clientName) throws Exception {
        TopicAndPartition topPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1));
        OffsetRequest req = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        kafka.javaapi.OffsetResponse resp = consumer.getOffsetsBefore(req);
        if(resp.hasError()) {
            logger.error("Error fetching data Offset Data the Broker. Reason: " + resp.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = resp.offsets(topic, partition);
        return offsets[0];
    }

    /**
     * get latest offset
     * @param consumer
     * @param topic
     * @param partition
     * @param clientName
     * @return
     * @throws Exception
     */
    private long getLatestOffset(SimpleConsumer consumer, String topic, int partition, String clientName) throws Exception {
        TopicAndPartition topPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
        OffsetRequest req = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        kafka.javaapi.OffsetResponse resp = consumer.getOffsetsBefore(req);
        if(resp.hasError()) {
            logger.error("Error fetching data Offset Data the Broker. Reason: " + resp.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = resp.offsets(topic, partition);
        return offsets[0];
    }

    /**
     * main process running
     * @param topic
     * @param partition
     * @param brokers
     * @param startOffset
     * @throws Exception
     */
    private void dump(String topic, int partition, List<String> brokers, long startOffset, long endOffset) throws Exception {
        PartitionMetadata metadata = findLeader(brokers, topic, partition);
        if(metadata == null) {
            throw new Exception("Can't find metadata for Topic and Partition. Exiting......");
        }
        if(metadata.leader() == null) {
            throw new Exception("Can't find Leader for Topic and Partition. Exiting......");
        }
        String leadBroker = metadata.leader().host();
        int port = metadata.leader().port();
        String clientName = "clientAWEFAWEFEGTA" + System.currentTimeMillis();
        SimpleConsumer consumer = createSimpleConsumer(leadBroker, port, partition, topic, clientName);
        long readOffset = startOffset;
        int errors = 0;
        long minOffset = getEarliestOffset(consumer, topic, partition, clientName);
        long maxOffset = getLatestOffset(consumer, topic, partition, clientName);
        logger.info("min offset = " + minOffset + ", read offset = " + readOffset + ", max offset = " + maxOffset + ", fetch size = " + consumerFetchSize);
        while (running) {
            if(consumer == null) {
                consumer = createSimpleConsumer(leadBroker, port, partition, topic, clientName);
            }
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(topic, partition, readOffset, consumerFetchSize) //default is 100000
                    .build();
            FetchResponse rep = consumer.fetch(req);
            if(rep.hasError()) {
                logger.error("simple consumer response has error code : " + rep.errorCode(topic, partition));
                errors++;
                short code = rep.errorCode(topic, partition);
                if(errors > 5) break;
                if(code == ErrorMapping.OffsetOutOfRangeCode()) {
                    minOffset = getEarliestOffset(consumer, topic, partition, clientName);
                    maxOffset = getLatestOffset(consumer, topic, partition, clientName);
                    logger.info("offset out of range, min offset = " + minOffset + ", read offset = " + readOffset + ", max offset = " + maxOffset);
                    if(readOffset < minOffset) {
                        readOffset = minOffset;
                    } else {
                        readOffset = maxOffset;
                    }
                    logger.info("reset the read offset to " + readOffset);
                    continue;
                }
                consumer.close();
                consumer = null;
                String brokerInfo = findNewLeader(leadBroker, port, topic, partition);
                String[] ss = brokerInfo.split(":");
                leadBroker = ss[0];
                port = Integer.valueOf(ss[1]);
            } else {
                errors = 0;
                long numRead = 0;
                for(MessageAndOffset message : rep.messageSet(topic, partition)) {
                    long currentOffset = message.offset();
                    if(currentOffset > endOffset) {
                        running = false;
                        logger.info("!!!!!!!!!!!!topic : " + topic + ", partition : " + partition + " have consumed end offset :" + endOffset);
                        break;
                    }
                    if(currentOffset < readOffset) {
                        logger.error("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                        continue;
                    }
                    readOffset = message.nextOffset();
                    ByteBuffer keyload = message.message().key();
                    byte[] key = new byte[keyload.limit()];
                    keyload.get(key);
                    ByteBuffer payload = message.message().payload();
                    byte[] value = new byte[payload.limit()];
                    payload.get(value);
                    KafkaData data = new KafkaData(currentOffset, key, value, partition, topic);
                    queue.put(data);
                    numRead++;
                }
                if(numRead == 0) {
                    Thread.sleep(5000);
                } else {
                    logger.debug("consume " + numRead + " messages");
                }
            }
        }
        if(consumer != null) {
            consumer.close();
        }
        running = false;
    }

    /**
     * stop the consumer
     * @throws Exception
     */
    public void stop() throws Exception {
        logger.info("stopping the simple consumer......");
        running = false;
    }

    /**
     * set the blocking queue
     * @param queue
     */
    public void setQueue(BlockingQueue<KafkaData> queue) {
        this.queue = queue;
    }

    /**
     * get the blocking queue
     * @return
     */
    public BlockingQueue<KafkaData> getQueue() {
        return queue;
    }

    /**
     * is running for consumer
     * @return boolean
     */
    public boolean isRunning() {
        return running; //Thread.isAlive() maybe thread do not start
    }
}
