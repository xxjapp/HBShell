package utils;

import static common.Common.getHomeDir;
import static common.Common.isEmpty;
import static common.Common.str2bytes;

import java.awt.event.KeyEvent;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;

import common.Common;
import common.Settings;
import main.HBShell;

public class Utils {
    private static final Log log = RootLog.getLog();

    private static final String UTF_8 = "UTF-8";

    //
    // os
    //

    public static boolean isWindows() {
        String os = System.getProperty("os.name");
        return (os != null) && os.startsWith("Windows");
    }

    //
    // file
    //

    public static boolean fileExists(String filePath) {
        File file = new File(filePath);
        return file.exists();
    }

    public static boolean renameFile(String from, String to) {
        File fileFrom = new File(from);
        File fileTo   = new File(to);

        return fileFrom.renameTo(fileTo);
    }

    public static boolean deleteFile(String filePath) {
        File file = new File(filePath);
        return file.delete();
    }

    public static String makePath(String parentPath, String name) {
        File file = new File(parentPath, name);
        return file.getPath();
    }

    public interface FoundLine {
        // return true to end search, false to continue
        boolean foundLine(String line);
    }

    public static void searchFileFromEnd(String fileName, FoundLine foundLine)
    throws IOException {
        File       file       = new File(fileName);
        long       fileLength = file.length();
        List<Byte> bytes      = new ArrayList<>();
        boolean    endSearch  = false;

        try (RandomAccessFile rf = new RandomAccessFile(file, "r")) {
            // read from end
            for (long filePointer = fileLength - 1; filePointer != -1; filePointer--) {
                rf.seek(filePointer);

                int b = rf.readByte();

                if (b == '\r') {
                    continue;
                } else if (b == '\n') {
                    Collections.reverse(bytes);
                    endSearch = foundLine.foundLine(bytes2str(byteListtoArray(bytes)));

                    if (endSearch) {
                        break;
                    }

                    // prepare to collect another line
                    bytes = new ArrayList<>();
                } else {
                    bytes.add((byte) b);
                }
            }

            if (!endSearch) {
                // first line of the file
                Collections.reverse(bytes);
                foundLine.foundLine(bytes2str(byteListtoArray(bytes)));
            }
        }
    }

    private static byte[] byteListtoArray(List<Byte> list) {
        int    len   = list.size();
        byte[] array = new byte[len];

        for (int i = 0; i < len; i++) {
            array[i] = list.get(i);
        }

        return array;
    }

    //
    // bytes
    //

    public static String bytes2str(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        try {
            return new String(bytes, UTF_8);
        } catch (UnsupportedEncodingException e) {
            log.error(null, e);
        }

        return null;
    }

    public static String bytes2str(byte[] bytes, int offset, int length) {
        if (bytes == null) {
            return null;
        }

        try {
            return new String(bytes, offset, length, UTF_8);
        } catch (UnsupportedEncodingException e) {
            log.error(null, e);
        }

        return null;
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

    private static boolean isPrintableChar(char c) {
        Character.UnicodeBlock block = Character.UnicodeBlock.of(c);

        if (Character.isWhitespace(c)) {
            return true;
        }

        return (!Character.isISOControl(c)) && 
               c != KeyEvent.CHAR_UNDEFINED && 
               block != null && 
               block != Character.UnicodeBlock.SPECIALS;
    }

    public static boolean isPrintableData(byte[] data, long maxPrintableDetectCnt) {
        for (int i = 0; i < data.length; i++) {
            if (i == maxPrintableDetectCnt) {
                break;
            }

            byte b = data[i];

            if (!isPrintableChar((char) b)) {
                return false;
            }
        }

        return true;
    }

    //
    // regexp
    //

    // example:
    // match("a0x12b", "a(\\d+)b") => []
    // match("a0012b", "a(\\d+)b") => [a0012b, 0012]
    public static List<String> match(String targetString, String patternString) {
        List<String> groups = new ArrayList<>();

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

    public static boolean isMatch(String target, String patternString) {
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(target);
        return matcher.find();
    }

    //
    // hbase
    //

    private static final String      HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final int         MAX_VERSIONS           = 1;
    private static Configuration     m_hBaseConfiguration   = null;
    private static final HConnection conn;

    static {
        try {
            conn = HConnectionManager.createConnection(conf());
        } catch (ZooKeeperConnectionException e) {
            throw new RuntimeException(e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    conn.close();
                } catch (IOException e) {
                    log.warn("faild to close connection to ZK");
                }
            }
        });
    }

    public static Configuration conf() {
        if (m_hBaseConfiguration == null) {
            setDefaultHBaseConfiguration();
        }

        return m_hBaseConfiguration;
    }

    public static String getQuorums() {
        return conf().get(HBASE_ZOOKEEPER_QUORUM);
    }

    public static void setQuorums(String quorums) {
        setDefaultHBaseConfiguration();
        m_hBaseConfiguration.set(HBASE_ZOOKEEPER_QUORUM, quorums);
    }

    private static void setDefaultHBaseConfiguration() {
        m_hBaseConfiguration = HBaseConfiguration.create();
        m_hBaseConfiguration.addResource(new Path(Settings.HBASE_SITE_XML));
    }

    public static HTableDescriptor[] listTables()
    throws IOException {
        try (HBaseAdmin admin = new HBaseAdmin(conf())) {
            return admin.listTables();
        }
    }

    // tableName

    public static HTableInterface getTable(String tableName)
    throws IOException {
        return conn.getTable(tableName);
    }

    public static boolean tableExists(String tableName)
    throws IOException {
        try (HBaseAdmin admin = new HBaseAdmin(conf())) {
            return admin.tableExists(tableName);
        }
    }

    public static void createTable(String tableName, List< ? > families)
    throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(str2bytes(tableName));

        for (Object family : families) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family.toString());
            columnDescriptor.setMaxVersions(MAX_VERSIONS);

            tableDescriptor.addFamily(columnDescriptor);
        }

        createTable(tableDescriptor);
    }

    private static void createTable(HTableDescriptor tableDescriptor)
    throws IOException {
        try (HBaseAdmin admin = new HBaseAdmin(conf())) {
            admin.createTable(tableDescriptor);
        }
    }

    public static void deleteTable(String tableName)
    throws IOException {
        log.info(tableName);

        try (HBaseAdmin admin = new HBaseAdmin(conf())) {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        }
    }

    // table

    private static String tableName(HTableInterface hTable) {
        return bytes2str(hTable.getTableName());
    }

    private static final Map<String, List<String>> familiesMap = new TreeMap<>();

    public static List<String> getFamilies(HTableInterface hTable)
    throws IOException {
        if (!HBShell.usefamilycache) {
            return getFamilies2(hTable);
        }

        String       tableName = tableName(hTable);
        List<String> families  = familiesMap.get(tableName);

        if (families == null) {
            families = readLocalFamilies(tableName);

            if (families == null) {
                families = getFamilies2(hTable);
                writeLocalFamilies(tableName, families);
            }

            familiesMap.put(tableName, families);
        }

        return families;
    }

    private static void writeLocalFamilies(String tableName, List<String> families)
    throws IOException {
        String familyInfo = Common.join(families.toArray(), "\n");
        FileUtils.writeStringToFile(localFamilyFile(tableName), familyInfo, UTF_8);
    }

    private static List<String> readLocalFamilies(String tableName) {
        try {
            String familyInfo = FileUtils.readFileToString(localFamilyFile(tableName), UTF_8);

            if (!isEmpty(familyInfo)) {
                return Arrays.asList(familyInfo.split("\n"));
            }
        } catch (IOException e) {
            // OK?
        }

        return null;
    }

    public static void clearFamilyCache() {
        familiesMap.clear();

        String path = String.format("%s/.hbshell/_families", getHomeDir());
        FileUtils.deleteQuietly(new File(path));
    }

    private static File localFamilyFile(String tableName) {
        String path = String.format("%s/.hbshell/_families/%s.txt", getHomeDir(), tableName);
        return new File(path);
    }

    private static List<String> getFamilies2(HTableInterface hTable)
    throws IOException {
        List<String> families = new ArrayList<>();

        HTableDescriptor descriptor = hTable.getTableDescriptor();

        for (byte[] bFamily : descriptor.getFamiliesKeys()) {
            families.add(bytes2str(bFamily));
        }

        return families;
    }

    public static String get(HTableInterface table, String key, String family, String qualifier)
    throws IOException {
        byte[] bRow       = str2bytes(key);
        byte[] bFamily    = str2bytes(family);
        byte[] bQualifier = str2bytes(qualifier);

        Get get = new Get(bRow);
        get.addColumn(bFamily, bQualifier);

        // get result
        Result result = null;

        try {
            result = table.get(get);
        } catch (NoSuchColumnFamilyException e) {
            // make error clear
            throw new NoSuchColumnFamilyException(family);
        }

        if (result.isEmpty()) {
            throw new NoSuchColumnFamilyException(family + ":" + qualifier);
        }

        byte[] bValue = result.getValue(bFamily, bQualifier);
        return bytes2str(bValue);
    }

    public static void put(HTableInterface hTable, String rowKey, String family, String qualifier, String value)
    throws IOException {
        log.info(rowKey + "/" + family + ":" + qualifier + " = " + value);

        Put put = new Put(str2bytes(rowKey));
        put.add(str2bytes(family), str2bytes(qualifier), str2bytes(value));
        hTable.put(put);
    }

    public static void put(HTableInterface hTable, String rowKey, String family, String qualifier, byte[] bValue)
    throws IOException {
        log.info(rowKey + "/" + family + ":" + qualifier + " = " + "0x...");

        Put put = new Put(str2bytes(rowKey));
        put.add(str2bytes(family), str2bytes(qualifier), bValue);
        hTable.put(put);
    }

    public static void deleteRow(HTableInterface hTable, String rowKey)
    throws IOException {
        log.info(rowKey);

        Delete delete = new Delete(str2bytes(rowKey));
        hTable.delete(delete);
    }

    public static void deleteFamily(HTableInterface hTable, String rowKey, String family)
    throws IOException {
        log.info(rowKey + "/" + family);

        Delete delete = new Delete(str2bytes(rowKey));
        delete.deleteFamily(str2bytes(family));
        hTable.delete(delete);
    }

    public static void deleteQualifier(HTableInterface hTable, String rowKey, String family, String qualifier)
    throws IOException {
        log.info(rowKey + "/" + family + ":" + qualifier);

        Delete delete = new Delete(str2bytes(rowKey));
        delete.deleteColumns(str2bytes(family), str2bytes(qualifier));
        hTable.delete(delete);
    }
}
