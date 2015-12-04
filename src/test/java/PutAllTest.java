import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: hackerwin7
 * Date: 2015/12/04
 * Time: 11:47 AM
 * Desc:
 */
public class PutAllTest {

    private static Map<String, String> ms1 = new HashMap<String, String>();
    private static Map<String, String> ms2 = new HashMap<String, String>();

    public static void main(String[] args) throws Exception {
        ms1.put("s1", "s1");
        ms2.putAll(ms1);
        ms1.put("s11", "s11");
        System.out.println(ms2);
    }
}
