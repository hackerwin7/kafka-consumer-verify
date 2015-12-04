import com.jd.bdp.jdq.auth.Authentication;
import com.jd.bdp.jdq.consumer.JDQSimpleConsumer;

/**
 * Created by IntelliJ IDEA.
 * User: hackerwin7
 * Date: 2015/12/04
 * Time: 10:18 AM
 * Desc:
 */
public class AppidTokenTest {

    private String appid = null;
    private String token = null;

    public void run(String[] args) throws Exception {
        JDQSimpleConsumer consumer = new JDQSimpleConsumer(new Authentication(appid, token));
    }
}
