package common;

import java.io.UnsupportedEncodingException;

public class Common {
    public static StackTraceElement getCallerInfo() {
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        return trace[3];
    }

    //
    // string
    //

    public static boolean isEmpty(String string) {
        return string == null || string.equals("");
    }

    public static String join(Object[] objects, String separator) {
        StringBuffer sb = new StringBuffer();

        for (Object object : objects) {
            sb.append(object + separator);
        }

        // delete last separator
        if (sb.length() > separator.length()) {
            sb.delete(sb.length() - separator.length(), sb.length());
        }

        return sb.toString();
    }

    //
    // bytes
    //

    public static final String UTF_8     = "UTF-8";
    public static final byte[] UTF_8_BOM = new byte[] { (byte)0xEF, (byte)0xBB, (byte)0xBF };

    public static String bytes2str(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        try {
            return new String(bytes, UTF_8);
        } catch (UnsupportedEncodingException e) {
            LogHelper.getLog().error(null, e);
        }

        return null;
    }

    // SEE: org.apache.hadoop.hbase.util.Bytes.toBytes(String)
    public static byte[] str2bytes(String string) {
        if (string == null) {
            return null;
        }

        try {
            return string.getBytes(UTF_8);
        } catch (UnsupportedEncodingException e) {
            LogHelper.getLog().error(null, e);
        }

        return null;
    }

    public static String removeUtf8Bom(String string) {
        byte[] bytes = str2bytes(string);

        if (bytes.length >= UTF_8_BOM.length && bytes[0] == UTF_8_BOM[0] && bytes[1] == UTF_8_BOM[1] && bytes[2] == UTF_8_BOM[2]) {
            byte[] bytes2 = new byte[bytes.length - UTF_8_BOM.length];
            System.arraycopy(bytes, UTF_8_BOM.length, bytes2, 0, bytes2.length);
            string = bytes2str(bytes2);
        }

        return string;
    }

    //
    // path
    //

    public static String getTmpDir() {
        return System.getProperty("java.io.tmpdir");
    }
}
