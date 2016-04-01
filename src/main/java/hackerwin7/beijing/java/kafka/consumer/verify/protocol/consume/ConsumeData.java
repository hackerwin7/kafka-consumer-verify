package hackerwin7.beijing.java.kafka.consumer.verify.protocol.consume;

import com.jd.bdp.rdd.magpie.job.framework.impl.mysql.protocol.protobuf.EventEntry;
import hackerwin7.beijing.java.kafka.consumer.verify.driver.KafkaData;
import hackerwin7.beijing.java.kafka.consumer.verify.protocol.avro.EntryAvroUtils;
import hackerwin7.beijing.java.kafka.consumer.verify.protocol.avro.EventEntryAvro;
import hackerwin7.beijing.java.kafka.consumer.verify.protocol.protobuf.EntryData;
import hackerwin7.beijing.java.kafka.consumer.verify.protocol.protobuf.EntryProtobufUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * uniform consume data
 * Created by hp on 9/23/15.
 */
public class ConsumeData {

    /*logger*/
    private static final Logger logger = Logger.getLogger(ConsumeData.class);

    /*mysql*/
    private String ip = null;
    private String dbname = null;
    private String tbname = null;
    private long timestamp = 0;
    private String type = null;
    private Map<String, String> cur = new HashMap<String, String>();
    private Map<String, String> src = new HashMap<String, String>();
    private Map<String, String> cus = new HashMap<String, String>();
    private List<Map<String, String>> srcs = new ArrayList<Map<String, String>>();
    private List<Map<String, String>> curs = new ArrayList<Map<String, String>>();


    /*kafka*/
    private long offset = 0;
    private String topic = null;
    private long partitionNum = 0;

    private String kKey = null;

    private byte[] value = null;

    /*data*/
    private long mid = 0;
    private String prePos = null;
    private String curPos = null;
    private String checknode = null;

    /*string*/
    private String strVal = null;

    /*consume type*/
    private ConsumeType consumeType = null;



    /**
     * get header info
     * @return
     */
    public String getHeader() {
        StringBuilder res = new StringBuilder();
        res.append("==================> header :\n");
        res.append("-----> topic = ").append(topic).append(", ")
                .append("partition = ").append(partitionNum).append(", ")
                .append("offset = ").append(offset).append(", ")
                .append("key = ").append(kKey).append(", ")
                .append("ip = ").append(ip).append(", ")
                .append("db = ").append(dbname).append(", ")
                .append("tb = ").append(tbname).append(", ")
                .append("timestamp = ").append(timestamp).append(", ")
                .append("type = ").append(type).append(", ").append(", ")
                .append("mid = ").append(mid)
                .append("value length = ").append(value.length);
        return res.toString();

    }

    /**
     * get data info
     * @return
     */
    public String getData() {
        StringBuilder res = new StringBuilder();
        res.append("==================> " + consumeType + " data :\n");
        if(consumeType == ConsumeType.PROTOBUF) {
            res.append("-------> src = " + srcs + "\n");
            res.append("-------> cur = " + curs + "\n");
        } else if(consumeType == ConsumeType.AVRO || consumeType == ConsumeType.ROW_PROTOBUF) {
            res.append("-------> src = " + src + "\n");
            res.append("-------> cur = " + cur + "\n");
        } else if(consumeType == ConsumeType.STRING) {
            res.append("-------> string val = " + strVal + "\n");
        } else {
            /* no op */
        }
        if(consumeType == ConsumeType.AVRO) {
            res.append("-------> cus = " + cus + "\n");
            res.append("-------> check = " + checknode + "\n");
        }
        if(consumeType == ConsumeType.ROW_PROTOBUF) {
            res.append("-------> pre pos = " + prePos + "\n");
            res.append("-------> cur pos = " + curPos + "\n");
        }
        res.delete(res.length() - 1, res.length());
        return res.toString();
    }

    /**
     * parse kafka data to consume data
     * @param cType
     * @param kd
     * @return cconsume data
     * @throws Exception
     */
    public static ConsumeData parseFrom(String cType, KafkaData kd) throws Exception {
        ConsumeData data = new ConsumeData();
        if(StringUtils.equalsIgnoreCase(cType, ConsumeType.AVRO.toString())) {
            data.setTopic(kd.getTopic());
            data.setPartitionNum(kd.getPartition());
            data.setOffset(kd.getOffset());
            if(kd.getKey() == null)
                data.setkKey(null);
            else
                data.setkKey(new String(kd.getKey()));
            byte[] value = kd.getValue();
            data.setValue(value);
            EventEntryAvro entry = EntryAvroUtils.bytes2Avro(value);
            if(entry == null) {
                return null;
            }
            logger.debug("avro entry = " + entry);
            Map<String, String> cussp = new HashMap<String, String>();
            for(Map.Entry<CharSequence, CharSequence> elem : entry.getCus().entrySet()) {
                String key = elem.getKey().toString();
                String val = elem.getValue().toString();
                cussp.put(key, val);
            }
            data.setIp(cussp.get("ip"));
            data.setDbname(entry.getDb().toString());
            data.setTbname(entry.getTab().toString());
            if(cussp.containsKey("ft")) {
                data.setTimestamp(Long.valueOf(cussp.get("ft")));
            }
            if(cussp.containsKey("dmlts")) {
                data.setTimestamp(Long.valueOf(cussp.get("dmlts")));
            }
            data.setType(entry.getOpt().toString());
            if(entry.getSrc() != null) { // src mayby null
                for (Map.Entry<CharSequence, CharSequence> et : entry.getSrc().entrySet()) {
                    String key = "";
                    if (et.getKey() != null) {
                        key = et.getKey().toString();
                    }
                    String val = "";
                    if (et.getValue() != null) {
                        val = et.getValue().toString();
                    }
                    data.getSrc().put(key, val);
                }
            }
            if(entry.getCur() != null) {
                for (Map.Entry<CharSequence, CharSequence> et : entry.getCur().entrySet()) {
                    String key = "";
                    if (et.getKey() != null) {
                        key = et.getKey().toString();
                    }
                    String val = "";
                    if (et.getValue() != null) {
                        val = et.getValue().toString();
                    }
                    data.getCur().put(key, val);
                }
            }
            if(entry.getCus() != null) {
                for (Map.Entry<CharSequence, CharSequence> et : entry.getCus().entrySet()) {
                    String key = "";
                    if (et.getKey() != null) {
                        key = et.getKey().toString();
                    }
                    String val = "";
                    if (et.getValue() != null) {
                        val = et.getValue().toString();
                    }
                    data.getCus().put(key, val);
                }
            }
            data.setMid(entry.getMid());
            data.setChecknode(cussp.get("check"));
            data.setConsumeType(ConsumeType.AVRO);
        } else if (StringUtils.equalsIgnoreCase(cType, ConsumeType.PROTOBUF.toString())) {
            data.setTopic(kd.getTopic());
            data.setPartitionNum(kd.getPartition());
            data.setOffset(kd.getOffset());
            if(kd.getKey() == null)
                data.setkKey(null);
            else
                data.setkKey(new String(kd.getKey()));
            byte[] value = kd.getValue();
            data.setValue(value);
            EntryData.Entry entry = EntryProtobufUtils.bytes2Protobuf(value);
            if(entry == null) {
                return null;
            }
            data.setIp(entry.getIp());
            data.setDbname(entry.getHeader().getSchemaName());
            data.setTbname(entry.getHeader().getTableName());
            data.setTimestamp(entry.getHeader().getExecuteTime());
            data.setType(entry.getHeader().getEventType().toString());
            EntryData.RowChange rowChange = EntryData.RowChange.parseFrom(entry.getStoreValue());
            for(EntryData.RowData rowData : rowChange.getRowDatasList()) {
                List<EntryData.Column> beforeCols = rowData.getBeforeColumnsList();
                List<EntryData.Column> afterCols = rowData.getAfterColumnsList();
                data.getSrc().clear();
                data.getCur().clear();
                Map<String, String> tcur = new HashMap<String, String>();
                Map<String, String> tsrc = new HashMap<String, String>();
                for(EntryData.Column column : beforeCols) {
                    tsrc.put(column.getName(), column.getValue());
                }
                for(EntryData.Column column : afterCols) {
                    tcur.put(column.getName(), column.getValue());
                }
                data.getSrc().putAll(tsrc);
                data.getCur().putAll(tcur);
                data.getCurs().add(tcur);
                data.getSrcs().add(tsrc);
                data.setConsumeType(ConsumeType.PROTOBUF);
            }
        } else if (StringUtils.equalsIgnoreCase(cType, ConsumeType.ROW_PROTOBUF.toString())) {
            data.setTopic(kd.getTopic());
            data.setPartitionNum(kd.getPartition());
            data.setOffset(kd.getOffset());
            if(kd.getKey() == null)
                data.setkKey(null);
            else
                data.setkKey(new String(kd.getKey()));
            byte[] value = kd.getValue();
            data.setValue(value);
            EventEntry.RowMsg rowMsgEntry = EventEntry.RowMsg.parseFrom(value);
            if(rowMsgEntry == null) {
                return null;
            }
            data.setIp(rowMsgEntry.getProps(0).getValue());
            data.setDbname(rowMsgEntry.getHeader().getSchemaName());
            data.setTbname(rowMsgEntry.getHeader().getTableName());
            data.setTimestamp(rowMsgEntry.getHeader().getExecuteTime());
            data.setType(rowMsgEntry.getEventType().toString());
            EventEntry.RowData rowData = rowMsgEntry.getRow();
            List<EventEntry.Column> beforeCols = rowData.getBeforeColumnsList();
            List<EventEntry.Column> afterCols = rowData.getAfterColumnsList();
            for(EventEntry.Column column : beforeCols) {
                data.getSrc().put(column.getName(), column.getValue());
            }
            for(EventEntry.Column column : afterCols) {
                data.getCur().put(column.getName(), column.getValue());
            }
            data.setPrePos(pos2Str(rowMsgEntry.getLastPos()));
            data.setCurPos(pos2Str(rowMsgEntry.getCurPos()));
            data.setConsumeType(ConsumeType.ROW_PROTOBUF);
        } else if(StringUtils.equalsIgnoreCase(cType, ConsumeType.STRING.toString())) {
            data.setTopic(kd.getTopic());
            data.setPartitionNum(kd.getPartition());
            data.setOffset(kd.getOffset());
            if(kd.getKey() == null)
                data.setkKey(null);
            else
                data.setkKey(new String(kd.getKey()));
            byte[] value = kd.getValue();
            data.setValue(value);
            data.setStrVal(new String(value));
            data.setConsumeType(ConsumeType.STRING);
        }
        else {
            /*no op*/
        }
        return data;
    }

    /**
     * position to string
     * @param pos
     * @return string
     * @throws Exception
     */
    private static String pos2Str(EventEntry.RowMsgPos pos) throws Exception {
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

    public List<Map<String, String>> getSrcs() {
        return srcs;
    }

    public List<Map<String, String>> getCurs() {
        return curs;
    }

    public void setSrcs(List<Map<String, String>> srcs) {
        this.srcs = srcs;
    }

    public void setCurs(List<Map<String, String>> curs) {
        this.curs = curs;
    }

    public String getChecknode() {
        return checknode;
    }

    public void setChecknode(String checknode) {
        this.checknode = checknode;
    }

    public String getPrePos() {
        return prePos;
    }

    public String getCurPos() {
        return curPos;
    }

    public void setPrePos(String prePos) {
        this.prePos = prePos;
    }

    public void setCurPos(String curPos) {
        this.curPos = curPos;
    }

    public void setCus(Map<String, String> cus) {
        this.cus = cus;
    }


    public Map<String, String> getCus() {
        return cus;
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    public long getMid() {
        return mid;
    }

    public void setMid(long mid) {
        this.mid = mid;
    }


    public String getTopic() {
        return topic;
    }

    public String getIp() {
        return ip;
    }

    public String getDbname() {
        return dbname;
    }

    public String getTbname() {
        return tbname;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getType() {
        return type;
    }

    public Map<String, String> getCur() {
        return cur;
    }

    public Map<String, String> getSrc() {
        return src;
    }

    public long getOffset() {
        return offset;
    }

    public long getPartitionNum() {
        return partitionNum;
    }

    public String getkKey() {
        return kKey;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setDbname(String dbname) {
        this.dbname = dbname;
    }

    public void setTbname(String tbname) {
        this.tbname = tbname;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setCur(Map<String, String> cur) {
        this.cur = cur;
    }

    public void setSrc(Map<String, String> src) {
        this.src = src;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setPartitionNum(long partitionNum) {
        this.partitionNum = partitionNum;
    }

    public void setkKey(String kKey) {
        this.kKey = kKey;
    }

    public void setStrVal(String val) {
        strVal = val;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
