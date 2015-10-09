package hackerwin7.beijing.java.kafka.consumer.verify.consume;

import com.jd.bdp.magpie.eggs.job.framework.impl.mysql.protocol.protobuf.EventEntry;
import hackerwin7.beijing.java.kafka.consumer.verify.driver.KafkaData;
import hackerwin7.beijing.java.kafka.consumer.verify.driver.KafkaSimpleConsumer;
import org.apache.commons.collections.CollectionUtils;
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
 * Created by hp on 9/15/15.
 */
public class RowDataProtobufConsume {
    private Logger logger = Logger.getLogger(RowDataProtobufConsume.class);

    private KafkaSimpleConsumer kafkaConsumer = null;

    private Set<String> ids = new HashSet<String>();

    private boolean running  = true;

    private String dbname = null;
    private String tbname = null;
    private String pKeys = null;
    private String pVals = null;
    private long endOffset = Long.MAX_VALUE;
    private String kvs = null;

    private int fetchSize = KafkaSimpleConsumer.FETCH_SIZE;

    /**
     * init the customer design for user
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
        kafkaConsumer = new KafkaSimpleConsumer(queue, zkServers, zkRoot, fetchSize);
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
            KafkaData data = queue.take();
            if(data.getOffset() > endOffset) {
                logger.info("reached end offset, exit the process ......");
                running = false;
                break;
            }
            byte[] value = data.getValue();
            EventEntry.RowMsg rowMsgEntry = EventEntry.RowMsg.parseFrom(value);
            String getDb = rowMsgEntry.getHeader().getSchemaName();
            String getTb = rowMsgEntry.getHeader().getTableName();
            //filter deal
            if(!isNULL(dbname)) {
                if(!getDb.equals(dbname)) {
                    continue;
                }
            }
            //filter deal
            if(!isNULL(tbname)) {
                if(!getTb.equals(tbname)) {
                    continue;
                }
            }
            //parse data to show
            StringBuilder sb = new StringBuilder();
            sb.append("------------------------------------------------------------------------------------------------------------------------\n");
            sb.append("-----------> kafka partition = " + data.getPartition() + ", offset = " + data.getOffset() + "\n");
            sb.append("-----------> header : dbname = " + getDb + ", tbname = " + getTb + ", type = " + rowMsgEntry.getEventType() + ", " +
                    "timestamp = " + rowMsgEntry.getHeader().getExecuteTime() + ", binlog = " + rowMsgEntry.getHeader().getLogfileName() +
                    ":" + rowMsgEntry.getHeader().getLogfileOffset() + "\n");
            if(rowMsgEntry.getIsDdl()) {
                sb.append(rowMsgEntry.getSql());
            } else { // row data
                //src
                sb.append("=========> src : " + columns2Str(rowMsgEntry.getRow().getBeforeColumnsList()) + "\n");
                //cur
                sb.append("=========> cur : " + columns2Str(rowMsgEntry.getRow().getAfterColumnsList()) + "\n");
                //pos
                sb.append("=========> pre pos : " + pos2Str(rowMsgEntry.getLastPos()) + "\n");
                sb.append("=========> cur pos : " + pos2Str(rowMsgEntry.getCurPos()) + "\n");
            }
            //filter deal for key value
            if(!StringUtils.isBlank(pKeys) || !StringUtils.isBlank(pVals)) {
                //key list
                String[] keyArr = StringUtils.split(pKeys, ",");
                List<String> keyList = new ArrayList<String>();
                CollectionUtils.addAll(keyList, keyArr);
                //value list
                String[] valArr = StringUtils.split(pVals, ",");
                List<String> valList = new ArrayList<String>();
                CollectionUtils.addAll(valList, valArr);
                //columns
                List<EventEntry.Column> srcCols = rowMsgEntry.getRow().getBeforeColumnsList();
                List<EventEntry.Column> curCols = rowMsgEntry.getRow().getAfterColumnsList();
                //if containCount > 0 then print info
                int containCount = 0;
                //check contain
                for(EventEntry.Column column : srcCols) {
                    if(keyList.contains(column.getName())) {
                        containCount++;
                    }
                    if(valList.contains(column.getValue())) {
                        containCount++;
                    }
                }
                if(containCount == 0) {
                    continue; // not print
                }
            }
            if(!StringUtils.isBlank(kvs)) {
                //kvs to map
                Map<String, String> kvMap = new HashMap<String, String>();// key : key , value : value
                String[] kvArr = StringUtils.split(kvs, ",");
                for(String kv : kvArr) {
                    String[] sar = StringUtils.split(kv, ":");
                    kvMap.put(sar[0], sar[1]);
                }
                //columns
                List<EventEntry.Column> srcCols = rowMsgEntry.getRow().getBeforeColumnsList();
                List<EventEntry.Column> curCols = rowMsgEntry.getRow().getAfterColumnsList();
                //if containCount > 0 then print info
                int containCount = 0;
                //check contain
                for (EventEntry.Column column : srcCols) {
                    if(kvMap.containsKey(column.getName()) && kvMap.get(column.getName()).equals(column.getValue())) {
                        containCount ++;
                    }
                }
                if(containCount == 0) {
                    continue;//do not print info
                }
            }
            //print info
            logger.info(sb.toString());
        }
    }

    /**
     * switch columns to string
     * @param columns
     * @return string
     * @throws Exception
     */
    private String columns2Str(List<EventEntry.Column> columns) throws Exception {
        StringBuilder res = new StringBuilder();
        List<String> kvs = new ArrayList<String>();
        res.append("[");
        for(EventEntry.Column column : columns) {
            kvs.add(column.getName() + ":" + column.getValue());
        }
        res.append(StringUtils.join(kvs, ","));
        res.append("]");
        return res.toString();
    }

    /**
     * position to string
     * @param pos
     * @return string
     * @throws Exception
     */
    private String pos2Str(EventEntry.RowMsgPos pos) throws Exception {
        StringBuilder res = new StringBuilder();
        res.append(pos.getLogFile()).append(":")
                .append(pos.getLogPos()).append(":")
                .append(pos.getServerId()).append(":")
                .append(pos.getTimestamp()).append(":")
                .append(pos.getTimestampId()).append(":")
                .append(pos.getRowId()).append(":")
                .append(pos.getUId());
        return res.toString();
    }

    /**
     * setter
     * @param dbname
     */
    public void setDbname(String dbname) {
        this.dbname = dbname;
    }

    /**
     * setter
     * @param tbname
     */
    public void setTbname(String tbname) {
        this.tbname = tbname;
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
     * @param pKeys
     */
    public void setpKeys(String pKeys) {
        this.pKeys = pKeys;
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

    /**
     * setter
     * @param fsize
     */
    public void setFetchSize(int fsize) throws Exception {
        if(fsize > 0) {
            fetchSize = fsize;
        }
    }

    /**
     * setter
     * @param kvStr
     * @throws Exception
     */
    public void setKvs(String kvStr) throws Exception {
        kvs = kvStr;
    }
}
