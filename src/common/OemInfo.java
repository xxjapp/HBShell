package common;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;

import org.apache.commons.logging.Log;

public class OemInfo {
    private static final Log    log              = LogHelper.getLog();
    private static final String CONF_SETTING_INI = "conf/setting.ini";

    private static OemInfo    instance   = null;         // the singleton instance
    private static Properties properties = null;

    private static OemInfo getInstance() {
        if (instance == null) {
            instance = new OemInfo();
            instance.getInfoFromINI();
        }

        return instance;
    }

    public static Properties getProperties() {
        if (properties == null) {
            properties = PropertiesHelper.getProperties(CONF_SETTING_INI);
        }

        return properties;
    }

    private String HBASE_URL  = "/home/hadoop/hbase/conf/hbase-site.xml";
    private String HBASE_URL2 = "./conf/hbase-site.xml";

    private void getInfoFromINI() {
        Properties properties = getProperties();

        HBASE_URL = PropertiesHelper.getProperty(properties, "HBASE_URL",           HBASE_URL);

        // check HBase setting file
        // use HBASE_URL2 if HBASE_URL not found
        try {
            HBASE_URL = checkHBaseSettingFile(HBASE_URL, HBASE_URL2);
        } catch (FileNotFoundException e) {
            log.error(null, e);
        }
    }

    private String checkHBaseSettingFile(String path, String path2)
    throws FileNotFoundException {
        File file = new File(path);

        if (file.exists()) {
            return path;
        }

        File file2 = new File(path2);

        if (file2.exists()) {
            log.info(path2);
            return path2;
        }

        throw new FileNotFoundException(path + ", " + path2);
    }

    public static String hbaseUrl() {
        OemInfo inst = getInstance();
        return inst.HBASE_URL;
    }
}
