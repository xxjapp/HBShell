package common;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class Common {
    public static Class< ? > getThisClass() {
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        String className = trace[2].getClassName();

        Class< ? > klass = null;

        try {
            klass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            LogHelper.getLog().error(null, e);
        }

        return klass;
    }

    public static StackTraceElement getCallerInfo() {
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        return trace[3];
    }

    public static String getClassFilePath(Class< ? > refClass) {
        ProtectionDomain pDomain = refClass.getProtectionDomain();
        CodeSource       cSource = pDomain.getCodeSource();
        URL              loc     = cSource.getLocation();

        return loc.getPath();
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

    public static String getTruncatedString(String string, int newLength) {
        if (string.length() <= newLength) {
            return string;
        }

        String extra = " ...";
        return string.substring(0, newLength - extra.length()) + extra;
    }

    public static String columnInCsv(Object obj) {
        return "\"" + obj + "\"" + ",";
    }

    public static String stringLast(String string, int length) {
        int stringLenght = string.length();
        return string.substring(stringLenght - length, stringLenght);
    }

    public static String characterString(int ch, int n) {
        if (n == 0) {
            return "";
        }

        return new String(new char[n]).replace('\0', (char) ch);
    }

    //
    // os
    //

    public static boolean isUnixLike() {
        return new File("/dev/null").exists();
    }

    //
    // time
    //

    // JAVA:    Thu May 23 14:18:03 JST 2013
    // --------
    // LOCAL:   Thu, 23 May 2013 14:18:03 JST
    // LOCAL2:  Thu, 23 May 2013 14:18:03 +0900
    // GMT:     Thu, 23 May 2013 05:18:03 GMT
    private static final SimpleDateFormat DATE_FORMAT_LOCAL  = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
    private static final SimpleDateFormat DATE_FORMAT_LOCAL2 = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z", Locale.US);
    private static final SimpleDateFormat DATE_FORMAT_GMT    = gmtDateFormat();

    public static Date datePlusDays(Date date, int nDay) {
        Calendar calendar = Calendar.getInstance();

        calendar.setTime(date);
        calendar.add(Calendar.DATE, nDay);

        return calendar.getTime();
    }

    private static SimpleDateFormat gmtDateFormat() {
        SimpleDateFormat dateFormat = (SimpleDateFormat) DATE_FORMAT_LOCAL.clone();
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat;
    }

    public static String gmtDateString(Date date) {
        if (date == null) {
            date = new Date();
        }

        return DATE_FORMAT_GMT.format(date);
    }

    public static String localDateString(Date date) {
        if (date == null) {
            date = new Date();
        }

        return DATE_FORMAT_LOCAL.format(date);
    }

    // LOCAL, LOCAL2, GMT -> LOCAL2
    public static String dateStringToLocal2(String dateString) {
        Date date = dateFromDateString(dateString);
        return DATE_FORMAT_LOCAL2.format(date);
    }

    // input =>
    // JAVA:    Thu May 23 14:18:03 JST 2013        Unparseable
    // --------
    // LOCAL:   Thu, 23 May 2013 14:18:03 JST       OK
    // LOCAL2:  Thu, 23 May 2013 14:18:03 +0900     OK
    // GMT:     Thu, 23 May 2013 05:18:03 GMT       OK
    public static Date dateFromDateString(String dateString) {
        try {
            return DATE_FORMAT_GMT.parse(dateString);
        } catch (ParseException e) {
            LogHelper.getLog().error(null, e);
            return null;
        }
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
    // regexp
    //

    public static List<String> match(String targetString, String patternString) {
        List<String> groups = new ArrayList<String>();

        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(targetString);

        if (matcher.find()) {
            int groupCount = matcher.groupCount();

            for (int i = 0; i < groupCount + 1; i++) {
                groups.add(matcher.group(i));
            }
        }

        return groups;
    }

    //
    // math
    //

    public static Object jsEval(String expression)
    throws ScriptException {
        ScriptEngine engine = getJsEngine();
        return engine.eval(expression);
    }

    private static ScriptEngine jsEngine = null;

    private static ScriptEngine getJsEngine() {
        if (jsEngine == null) {
            jsEngine = new ScriptEngineManager().getEngineByName("JavaScript");
        }

        return jsEngine;
    }

    //
    // path
    //

    public static String getTmpDir() {
        return System.getProperty("java.io.tmpdir");
    }

    public static String standardizePath(String path) {
        File file = new File(path);
        return unixStylePath(file.getPath());
    }

    public static String getParentPath(String path) {
        File   file       = new File(path);
        String parentPath = file.getParent();

        return unixStylePath(parentPath);
    }

    public static String makePath(String parentPath, String name) {
        File   file = new File(parentPath, name);
        String path = file.getPath();

        return unixStylePath(path);
    }

    private static String unixStylePath(String path) {
        // change "\\" -> "/" on windows
        return path.replaceAll("\\\\", "/");
    }

    public static String getName(String path) {
        File   file = new File(path);
        String name = file.getName();

        return name;
    }

    // --- fileName: null
    // BaseName:  null
    // Extension: null
    // --- fileName:
    // BaseName:
    // Extension: null
    // --- fileName: 1
    // BaseName:  1
    // Extension: null
    // --- fileName: .
    // BaseName:
    // Extension:    // (!=null if has ".")
    // --- fileName: 1.
    // BaseName:  1
    // Extension:
    // --- fileName: .2
    // BaseName:
    // Extension: 2
    // --- fileName: 1.2
    // BaseName:  1
    // Extension: 2
    // --- fileName: .1.2
    // BaseName:  .1
    // Extension: 2
    // --- fileName: 1.2.
    // BaseName:  1.2
    // Extension:
    // --- fileName: 1.2.3
    // BaseName:  1.2
    // Extension: 3
    // --- fileName: /xx/yy/1.2.3
    // BaseName: /xx/yy/1.2
    // Extension: 3

    public static String getFileBaseName(String fileName) {
        if (fileName == null) {
            return null;
        }

        String baseName = null;
        int    pos      = fileName.lastIndexOf(".");

        if (pos == -1) {
            // there wasn't any '.'
            baseName = fileName;
        } else {
            baseName = fileName.substring(0, pos);
        }

        return baseName;
    }

    public static String getFileExtension(String fileName) {
        if (fileName == null) {
            return null;
        }

        String extension = null;
        int    pos       = fileName.lastIndexOf(".");

        if (pos != -1) {
            if (pos != fileName.length() - 1) {
                extension = fileName.substring(pos + 1, fileName.length());
            } else {
                extension = "";
            }
        }

        return extension;
    }

    public static String getFirstLevelFolderPath(String path) {
        return "/" + getPathFirstLevelName(path);
    }

    public static String getPathFirstLevelName(String path) {
        String[] parts = path.split("/");
        return parts[1];
    }

    public static String getPathFromSecondeLevel(String path) {
        try {
            return path.substring(path.indexOf('/', 1));
        } catch (NullPointerException e) {
            // return null
        } catch (StringIndexOutOfBoundsException e) {
            // return null
        }

        return null;
    }

    // long

    public static Long longValue(String value) {
        if (value == null) {
            return null;
        }

        return Long.valueOf(value);
    }

    //
    // md5
    //

    public static String getHexString(byte[] bytes) {
        return getHexStringBase(bytes, bytes.length, false);
    }

    public static String getHexStringBase(byte[] bytes, int length, boolean show0x) {
        StringBuffer stringBuffer = new StringBuffer();

        length = Math.min(length, bytes.length);

        for (int i = 0; i < length; i++) {
            String hex = Integer.toHexString(0xFF & bytes[i]);

            if (show0x) {
                stringBuffer.append((i == 0 ? "" : " ") + "0x");
            }

            if (hex.length() == 1) {
                stringBuffer.append('0');
            }

            stringBuffer.append(hex);
        }

        if (length < bytes.length) {
            stringBuffer.append(" ...");
        }

        return stringBuffer.toString();
    }

    //
    // uuid
    //

    private static final long MAX_TIME = 0xFFFFFFFFFFFL + 1;                 // 557 years (millisecond)

    public static String timeBasedUUID(boolean descending) {
        String hexString = null;

        if (descending) {
            hexString = Long.toHexString(MAX_TIME - System.currentTimeMillis());
        } else {
            hexString = Long.toHexString(System.currentTimeMillis());
        }

        // get only last 11 hex
        // this operation has no effect in the future 514 years (557 - (2013 - 1970)) when descending
        // hexString = hexString.substring(hexString.length() - 11, hexString.length());

        String part1       = hexString.substring(0, 8);
        String part2_head3 = hexString.substring(8);
        String fragment    = part1 + "-" + part2_head3;

        String uuid   = UUID.randomUUID().toString();
        String myUuid = fragment + uuid.substring(fragment.length(), uuid.length());

        // examples:
        // ec25a89b-71b3-4a35-a42c-36b8f9f0fcf6
        // ec25a89b-71ad-404d-b0cd-57084035dc22
        // ~~~~~~~~-~~~ time part
        // ** time interval of one unit change **
        // |||||||||||^ 0.001 sec
        // ||||||||||^ 0.016 sec
        // |||||||||^ 0.256 sec
        // ||||||||-
        // |||||||^ 4 sec
        // ||||||^ 1 min
        // |||||^ 17 min
        // ||||^ 4 hour
        // |||^ 3 day
        // ||^ 2 month
        // |^ 2 year
        // ^ 35 year
        // ** end of time interval of one unit change **

        return myUuid;
    }

    public static String uuid() {
        return timeBasedUUID(true);
    }

    //
    // other
    //

    public static Map<String, String[]> getQueryParameterMap(String queryString)
    throws UnsupportedEncodingException {
        if (queryString == null) {
            return null;
        }

        Map<String, String[]> retMap = new HashMap<String, String[]>();

        for (String parameter : queryString.split("&")) {
            String[] parts = parameter.split("=");
            String name  = URLDecoder.decode(parts[0], Common.UTF_8);
            String value = null;

            try {
                value = URLDecoder.decode(parts[1], Common.UTF_8);
            } catch (Exception e) {
                LogHelper.getLog().warn(null, e);
            }

            String[] v = {value};
            retMap.put(name, v);
        }

        return retMap;
    }
}
