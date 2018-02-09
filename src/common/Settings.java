package common;

import java.util.Properties;

import utils.PropertiesHelper;

public class Settings {
    private static final String     CONF_SETTING_INI = "conf/setting.ini";
    private static final Properties properties       = PropertiesHelper.getPropertiesBase(CONF_SETTING_INI);
    public static final String      HBASE_SITE_XML   =  PropertiesHelper.getProperty(properties, "HBASE_SITE_XML", "/home/hadoop/hbase/conf/hbase-site.xml");
}
