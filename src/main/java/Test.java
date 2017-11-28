import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author jiangtao7
 * @date 2017/10/26
 */
public class Test {
    public static void main(String[] args) {
        Map map = new HashMap(16);
        String s = StringUtils.leftPad(Integer.toString(Math.abs(20 / 15)), 2, "0");
        System.out.println(s);
        String checkId = "A208849559 ";
        String md5AsHex = MD5Hash.getMD5AsHex(Bytes.toBytes(checkId));
        System.out.println(md5AsHex+" "+md5AsHex.length());

        byte[] stopRow = Bytes.toBytes(md5AsHex);
        stopRow[0]++;
        System.out.println(stopRow[0]);
    }
}
