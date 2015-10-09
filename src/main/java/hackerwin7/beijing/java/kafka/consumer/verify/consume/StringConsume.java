package hackerwin7.beijing.java.kafka.consumer.verify.consume;

import hackerwin7.beijing.java.kafka.consumer.verify.driver.KafkaData;
import hackerwin7.beijing.java.kafka.consumer.verify.driver.KafkaSimpleConsumer;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hp on 5/28/15.
 */
public class StringConsume {
    private Logger logger = Logger.getLogger(StringConsume.class);

    private KafkaSimpleConsumer kafkaConsumer = null;

    private String filterNot = null;

    private boolean running = true;

    private String filterOnly = null;
    private long endOffset = Long.MAX_VALUE;

    /**
     * init the filter
     * @throws Exception
     */
    private void init() throws Exception {

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
        while (!queue.isEmpty()) {
            KafkaData data = queue.take();
            if(data.getOffset() > endOffset) {
                logger.info("reached end offset, exit the process ......");
                running = false;
                break;
            }
            byte[] value = data.getValue();
            String strVal = new String(value);
            if(!isNULL(filterNot)) {
                if(strVal.contains(filterNot)) {
                    continue;
                }
            }
            if(!isNULL(filterOnly)) {
                if(!strVal.contains(filterOnly)) {
                    continue;
                }
            }
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append("offset : " + data.getOffset() + "||");
            sb.append("value : " + strVal);
            sb.append("}");
            logger.info("data = " + sb.toString());
        }
    }

    /**
     * setter filter not
     * @param filterNot
     */
    public void setFilterNot(String filterNot) {
        this.filterNot = filterNot;
    }

    /**
     * setter filter only
     * @param filterOnly
     */
    public void setFilterOnly(String filterOnly) {
        this.filterOnly = filterOnly;
    }

    /**
     * setter
     * @param endOffset
     */
    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    /**
     * str is "" or null , and trim it
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
