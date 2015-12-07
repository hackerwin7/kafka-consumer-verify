package hackerwin7.beijing.java.kafka.consumer.verify.driver;

/**
 * Created by IntelliJ IDEA.
 * User: hackerwin7
 * Date: 2015/12/04
 * Time: 4:04 PM
 * Desc: kafka machine information
 */
public class KafkaInfo {
    public String brokers = null;
    public String zks = null;
    public String topic = null;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("kafka clusters information : ").append("\n");
        sb.append("---> zks = ").append(zks).append("\n");
        sb.append("---> topic = ").append(topic).append("\n");
        sb.append("---> brokers = ").append(brokers);
        return sb.toString();
    }
}
