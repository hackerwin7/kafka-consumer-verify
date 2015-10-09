package hackerwin7.beijing.java.kafka.consumer.verify.protocol.avro;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;

/**
 * Created by hp on 5/22/15.
 */
public class EntryAvroUtils {
    private static Logger logger = Logger.getLogger(EntryAvroUtils.class);

    /**
     * parse object to bytes
     * @param avro
     * @return
     * @throws Exception
     */
    public static byte[] avro2Bytes(EventEntryAvro avro) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out,null);
        DatumWriter<EventEntryAvro> writer = new SpecificDatumWriter<EventEntryAvro>(EventEntryAvro.getClassSchema());
        writer.write(avro,encoder);
        encoder.flush();
        out.close();
        byte[] value = out.toByteArray();
        return value;
    }

    /**
     * parse bytes to object
     * @param value
     * @return
     * @throws Exception
     */
    public static EventEntryAvro bytes2Avro(byte[] value) throws Exception {
        SpecificDatumReader<EventEntryAvro> reader = new SpecificDatumReader<EventEntryAvro>(EventEntryAvro.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(value,null);
        EventEntryAvro avro = null;
        try {
            avro = reader.read(null,decoder);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return avro;
    }
}
