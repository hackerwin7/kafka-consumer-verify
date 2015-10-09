package hackerwin7.beijing.java.kafka.consumer.verify.process;

import hackerwin7.beijing.java.kafka.consumer.verify.consume.ProtobufConsume;

/**
 * Created by hp on 5/22/15.
 */
public class ProtobufHandler {
    public static void main(String[] args) throws Exception {
        String zks = args[0];
        String topic = args[1];
        int partition = Integer.valueOf(args[2]);
        long offset = Long.valueOf(args[3]);
        ProtobufConsume consume = new ProtobufConsume();
        consume.start(zks, topic, partition, offset);
    }
}
