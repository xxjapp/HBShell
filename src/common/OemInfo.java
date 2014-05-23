package common;

import java.util.Properties;

public class OemInfo {
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

    private String HBASE_SITE_XML = "/home/hadoop/hbase/conf/hbase-site.xml";

    private void getInfoFromINI() {
        Properties properties = getProperties();

        HBASE_SITE_XML = PropertiesHelper.getProperty(properties, "HBASE_SITE_XML", HBASE_SITE_XML);
    }

    public static String hbaseSiteXml() {
        OemInfo inst = getInstance();
        return inst.HBASE_SITE_XML;
    }
}
