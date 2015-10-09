package hackerwin7.beijing.java.kafka.consumer.verify.driver;

/**
 * Created by hp on 5/27/15.
 */
public class KafkaData {
    public byte[] getValue() {
        return value;
    }

    public long getOffset() {
        return offset;
    }

    public int getPartition() {
        return partition;
    }

    public String getTopic() {
        return topic;
    }

    private byte[] value;
    private long offset;
    private int partition;
    private String topic;

    public KafkaData(long _offset, byte[] _value, int _par, String _topic) {
        offset = _offset;
        value = _value;
        partition = _par;
        topic = _topic;
    }

}
