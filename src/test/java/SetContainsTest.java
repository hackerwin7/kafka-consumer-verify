import java.util.HashSet;
import java.util.Set;

/**
 * Created by hp on 8/31/15.
 */
public class SetContainsTest {
    public static void main(String[] args) throws Exception {
        Set<Long> longSet = new HashSet<Long>();
        longSet.add(1231313L);
        longSet.add(12568L);
        longSet.add(213L);
        longSet.add(7777L);
        longSet.add(112L);
        longSet.add(0L);
        long l1 = 213L;
        long l2 = 111122233L;
        System.out.println(longSet.contains(l1) + ", " + longSet.contains(l2));
    }
}
