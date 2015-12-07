package hackerwin7.beijing.java.kafka.consumer.verify.driver;

import com.depend.jdq.fastjson.JSON;
import com.depend.jdq.fastjson.JSONObject;
import com.depend.jdq.http.HttpEntity;
import com.depend.jdq.http.HttpResponse;
import com.depend.jdq.http.HttpStatus;
import com.depend.jdq.http.auth.AuthenticationException;
import com.depend.jdq.http.client.HttpClient;
import com.depend.jdq.http.client.methods.HttpGet;
import com.depend.jdq.http.impl.client.DefaultHttpClient;
import com.depend.jdq.http.util.EntityUtils;
import com.jd.bdp.jdq.auth.Authentication;
import com.jd.bdp.jdq.consumer.JDQSimpleConsumer;
import com.jd.bdp.jdq.consumer.simple.JDQSimpleMessage;
import com.jd.bdp.jdq.consumer.simple.OutOfRangeHandler;
import com.jd.bdp.jdq.consumer.simple.Partition;
import ex.JDQException;
import org.apache.log4j.Logger;

import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by IntelliJ IDEA.
 * User: hackerwin7
 * Date: 2015/12/04
 * Time: 10:04 AM
 * Desc: using app id and token to consumer the kafka
 */
public class AppidTokenConsumer {

    /*constants*/
    public static final int QUEUE_SIZE = 10000;
    public static final int SLEEPING_INTERVAL = 3000;//3 seconds
    public static final String DEFAULT_TOPIC = "apptoken_no_topic";
    public static final String AUTHENTICATION_URL = "http://train.bdp.jd.com/api/q/oauth2/authCustomer_v10.ajax?content=";
    public static final String APPID_STR = "{\"appId\":\"";
    public static final String TOKEN_STR = "\",\"token\":\"";
    public static final String IP_STR = "\",\"ip\":\"127.0.0.1\"}";
    public static final String DEFAULT_CHARSET_STR = "UTF-8";
    public static final String JDQ_AUTH_ZK_ROOT_KEY = "zkRoot";
    public static final String JDQ_AUTH_TOPIC_KEY = "topic";
    public static final String JDQ_AUTH_BROKERS_KEY = "brokers";
    public static final String JDQ_AUTH_OBJ_KEY = "obj";

    /*logger*/
    private Logger logger = Logger.getLogger(AppidTokenConsumer.class);

    /*data*/
    private String appid = null;
    private String token = null;
    private BlockingQueue<KafkaData> queue = new LinkedBlockingQueue<KafkaData>(QUEUE_SIZE);

    /*thread running*/
    private boolean running = true;
    private Thread thCon = null;

    /*driver*/
    private JDQSimpleConsumer consumer = null;

    /*consumer position*/
    private Map<Integer, Long> posPool = new HashMap<Integer, Long>();
    private Map<Integer, Long> endPosPool = new HashMap<Integer, Long>();
    private Map<Integer, Long> minPosPool = new HashMap<Integer, Long>();
    private Map<Integer, Long> maxPosPool = new HashMap<Integer, Long>();

    /**
     * get the kafka information
     * @param appid
     * @param token
     * @return kafka info
     */
    public static KafkaInfo getInfo(String appid, String token) throws Exception {
        String url = AUTHENTICATION_URL + URLEncoder.encode(APPID_STR + appid + TOKEN_STR + token + IP_STR, DEFAULT_CHARSET_STR);
        HttpClient client = new DefaultHttpClient();
        HttpGet get = null;
        get = new HttpGet(url);
        HttpResponse res = client.execute(get);
        if(res.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
            HttpEntity entity = res.getEntity();
            JSONObject jmsg = JSON.parseObject(EntityUtils.toString(entity, "UTF-8"));
            JSONObject jdata = jmsg.getJSONObject(JDQ_AUTH_OBJ_KEY);
            KafkaInfo info = new KafkaInfo();
            info.zks = jdata.getString(JDQ_AUTH_ZK_ROOT_KEY);
            info.topic = jdata.getString(JDQ_AUTH_TOPIC_KEY);
            info.brokers = jdata.getString(JDQ_AUTH_BROKERS_KEY);
            return info;
        } else {
            throw new AuthenticationException("conn failed, please config the correct host, error code = " + res.getStatusLine().getStatusCode());
        }
    }

    /**
     * consume the kafka using appid and token
     * @param _appid
     * @param _token
     */
    public AppidTokenConsumer(String _appid, String _token) {
        appid = _appid;
        token = _token;
        try {
            //construct consumer
            consumer = new JDQSimpleConsumer(new Authentication(appid, token));
            //default position set
            List<Partition> partitions = consumer.findPartitions();
            for(Partition partition : partitions) {
                int part = partition.getPartition();
                Long maxOffset = consumer.findValidityMaxOffset(part);
                Long minOffset = consumer.findValidityMinOffset(part);
                minPosPool.put(part, minOffset);
                maxPosPool.put(part, maxOffset);
                //end offset default
                endPosPool.put(part, Long.MAX_VALUE);
            }
            posPool.putAll(maxPosPool);
        } catch (JDQException e) {
            logger.error(e.getMessage(), e);
            logger.error("using appid = " + appid + ", token = " + token + " to init jdq consumer failed, exit system......");
            logger.info("exiting system......");
            System.exit(-1);
        }
    }

    /**
     * start the consumer
     * @throws Exception
     */
    public void start() throws Exception {
        logger.info("start the jdq consumer using appid = " + appid + ", token = " + token + " ......");
        logger.info("kafka info => " + getInfo(appid, token).toString());
        logger.info("min offsets => " + seeMap(minPosPool));
        logger.info("max offsets => " + seeMap(maxPosPool));
        logger.info("load offsets => " + seeMap(posPool));
        logger.info("after default strategy, reset the load offset......");
        offsetStrategy();
        logger.info("load offsets => " + seeMap(posPool));
        thCon = new Thread(new Runnable() {
            public void run() {
                try {
                    dump();
                } catch (Throwable e) {
                    logger.error(e.getMessage(), e);
                    logger.info("exiting system......");
                    System.exit(-1);
                } finally {
                    close();
                }
            }
        });
        thCon.start();
    }

    /**
     * consume the message into a queue and offer messages to the outer
     * @throws Throwable
     */
    private void dump() throws Throwable {
        while (running) {
            boolean continueZero = true;
            for(Map.Entry<Integer, Long> entry : posPool.entrySet()) {
                final int partition = entry.getKey();
                long offset = entry.getValue();
                //if consume the offset to the end offset, then do not consume
                if(offset > endPosPool.get(partition)) {
                    //if all partition arrive to end , end the consumer thread
                    if(isAllEnd()) {
                        logger.info("consume to the end offset, stopping consumer ......");
                        running = false;
                        break;
                    } else {
                        continue;
                    }
                }
                //consume a batch
                List<JDQSimpleMessage> messages = consumer.consumeMessage(partition, offset, new OutOfRangeHandler() {
                    public long handleOffsetOutOfRange(long l, long l1, long l2) {
                        logger.error("out of range with offset min = " + l + ", max = " + l1 + ", request = " + l2);
                        long closest = 0;
                        if(l2 < l)
                            closest = l;
                        else if(l2 > l1)
                            closest = l1;
                        else
                            closest = l2;
                        logger.info("reset to the closest offset = " + closest + ", in partition = " + partition);
                        try {
                            Thread.sleep(SLEEPING_INTERVAL);
                        } catch (InterruptedException e) {
                            logger.error(e.getMessage(), e);
                        }
                        return closest;
                    }
                });
                if(messages.size() > 0) {
                    //set symbol
                    continueZero = false;
                    JDQSimpleMessage endMsg = null;
                    //put message to queue
                    for(JDQSimpleMessage msg : messages) {
                        endMsg = msg;
                        if(msg.getOffset() > endPosPool.get(partition))
                            break;
                        KafkaData data = new KafkaData(msg.getOffset(), msg.getKey(), msg.getData(), msg.getPartition(), DEFAULT_TOPIC);
                        queue.put(data);
                    }
                    //update position map offset (update next offset)
                    posPool.put(partition, endMsg.getNextOffset());
                }
                logger.debug("consumed " + messages.size() + " messages with partition = " + partition + ", offset = " + offset);
            }
            if(continueZero) {
                Thread.sleep(SLEEPING_INTERVAL);
            }
        }
    }

    private void offsetStrategy() {
        for(Map.Entry<Integer, Long> entry : posPool.entrySet()) {
            int partition = entry.getKey();
            long offset = entry.getValue();
            long minOffset = minPosPool.get(partition);
            long maxOffset = maxPosPool.get(partition);
            long endOffset = endPosPool.get(partition);
            //offset
            if(offset < minOffset) {
                posPool.put(partition, minOffset);
            } else if(offset > maxOffset) {
                posPool.put(partition, maxOffset);
            } else {
                /*no op*/
            }
            //update
            offset = posPool.get(partition);
            //end offset
            if(endOffset < offset) {
                endPosPool.put(partition, Long.MAX_VALUE);
            }
            //update
            endOffset = endPosPool.get(partition);
        }
    }

    /**
     * all partition consume to end offset
     */
    private boolean isAllEnd() {
        for(Map.Entry<Integer, Long> entry : posPool.entrySet()) {
            int partition = entry.getKey();
            long offset = entry.getValue();
            long endOffset = endPosPool.get(partition);
            if(offset <= endOffset)
                return false;
        }
        return true;
    }

    /**
     * see map value
     * @param map
     * @return map value string
     */
    private String seeMap(Map<Integer, Long> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        for(Map.Entry<Integer, Long> entry : map.entrySet()) {
            int partition = entry.getKey();
            long offset = entry.getValue();
            sb.append("[partition = " + partition + " | offset = " + offset + "]").append("\n");
        }
        return sb.toString();
    }

    /**
     * get consumer thread state
     */
    public String getState() {
        return thCon.getState().toString();
    }

    /**
     * is dump running
     * @return running status
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * set running status
     * @param _running
     */
    public void setRunning(boolean _running) {
        running = _running;
    }

    /**
     * take kafka data
     * @return kafka data message
     * @throws Exception
     */
    public KafkaData take() throws Exception {
        return queue.take();
    }

    /**
     * is empty for queue
     * @return bool
     */
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    /**
     * get queue length
     * @return blocking queue's size
     */
    public int getSize() {
        return queue.size();
    }

    /**
     * put the parittion and offset kv
     * @param partition
     * @param offset
     */
    public void putPos(int partition, long offset) {
        posPool.put(partition, offset);
    }

    /**
     * put the partition and end offset kv
     * @param partition
     * @param endOffset
     */
    public void putEndPos(int partition, long endOffset) {
        endPosPool.put(partition, endOffset);
    }

    /**
     * set blocking queue, it is not properly
     * @param _queue
     */
    public void setQueue(BlockingQueue<KafkaData> _queue) {
        queue = _queue;
    }

    /**
     * close the jdq consumer
     * @throws Exception
     */
    public void close() {
        if(consumer != null)
            consumer.close();
    }
}
