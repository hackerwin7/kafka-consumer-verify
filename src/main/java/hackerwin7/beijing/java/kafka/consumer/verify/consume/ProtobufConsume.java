package hackerwin7.beijing.java.kafka.consumer.verify.consume;

import hackerwin7.beijing.java.kafka.consumer.verify.driver.KafkaData;
import hackerwin7.beijing.java.kafka.consumer.verify.driver.KafkaSimpleConsumer;
import hackerwin7.beijing.java.kafka.consumer.verify.protocol.protobuf.EntryData;
import hackerwin7.beijing.java.kafka.consumer.verify.protocol.protobuf.EntryProtobufUtils;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * consume the data from tracker to parser
 * Created by hp on 5/22/15.
 */
public class ProtobufConsume {
    private Logger logger = Logger.getLogger(ProtobufConsume.class);

    private KafkaSimpleConsumer kafkaConsumer = null;

    private Set<String> ids = new HashSet<String>();

    private boolean running  = true;

    private String dbname = null;
    private String tbname = null;
    private String pKey = null;
    private String pVals = null;
    private long endOffset = Long.MAX_VALUE;

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
            EntryData.Entry entry = EntryProtobufUtils.bytes2Protobuf(value);
            String getDb = entry.getHeader().getSchemaName();
            String getTb = entry.getHeader().getTableName();
            if(!isNULL(dbname)) {
                if(!getDb.equals(dbname)) {
                    continue;
                }
            }
            if(!isNULL(tbname)) {
                if(!getTb.equals(tbname)) {
                    continue;
                }
            }
            EntryData.RowChange rowChange = EntryData.RowChange.parseFrom(entry.getStoreValue());
            if(rowChange.getIsDdl()) {
                logger.info("ddl = " + rowChange.getSql());
            } else {
                tag1:
                for (EntryData.RowData rowData : rowChange.getRowDatasList()) {
                    //src
                    List<EntryData.Column> columnList = null;
                    StringBuilder sb = new StringBuilder();
                    sb.append("============> src: {");
                    int keyCnt0 = 0;
                    columnList = rowData.getBeforeColumnsList();
                    for(EntryData.Column column : columnList) {
                        String fieldName = column.getName();
                        String fieldValue = column.getValue();
                        if(!isNULL(pKey)) {
                            if(fieldName.equals(pKey)) {
                                if(ids.size() > 0 && !ids.contains(fieldValue)) {
                                    continue tag1;
                                }
                                keyCnt0++;
                            }
                        }
                        sb.append(column.getName() + ":" + column.getValue() + ",");
                    }
                    if(!isNULL(pKey) && keyCnt0 <= 0) {
                        continue;
                    }
                    sb.append("}\n");
                    //cur
                    sb.append("============> cur: {");
                    int keyCnt = 0;
                    columnList = rowData.getAfterColumnsList();
                    for(EntryData.Column column : columnList) {
                        String fieldName = column.getName();
                        String fieldValue = column.getValue();
                        if(!isNULL(pKey)) {
                            if(fieldName.equals(pKey)) {
                                if(ids.size() > 0 && !ids.contains(fieldValue)) {
                                    continue tag1;
                                }
                                keyCnt++;
                            }
                        }
                        sb.append(column.getName() + ":" + column.getValue() + ",");
                    }
                    if(!isNULL(pKey) && keyCnt <= 0) {
                        continue;
                    }
                    sb.append("}");
                    logger.info("protobuf entry dbName = " + entry.getHeader().getSchemaName() + ", tbName = " + entry.getHeader().getTableName() + ", type = " + entry.getHeader().getEventType() + ", offset = " + data.getOffset());
                    logger.info("row data = " + sb.toString());
                }
            }
        }
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

    /**
     * setter
     * @param fsize
     */
    public void setFetchSize(int fsize) throws Exception {
        if(fsize > 0) {
            fetchSize = fsize;
        }
    }
}
