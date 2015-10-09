package hackerwin7.beijing.java.kafka.consumer.verify.protocol.protobuf;

/**
 * Created by hp on 5/22/15.
 */
public class EntryProtobufUtils {

    public static byte[] protobuf2Bytes(EntryData.Entry entry) throws Exception {
        return entry.toByteArray();
    }

    public static EntryData.Entry bytes2Protobuf(byte[] value) throws Exception {
        return EntryData.Entry.parseFrom(value);
    }
}
