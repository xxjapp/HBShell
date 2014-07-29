package common;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;

public class PropertiesHelper {
    private static final Log log = LogHelper.getLog();

    //
    // getProperties
    //

    public static Properties getProperties(String iniFilePath) {
        log.info(iniFilePath);
        Properties properties = new Properties();

        try {
            properties.load(new FileInputStream(iniFilePath));
        } catch (IOException e) {
            log.warn("Couldn't open setting file(" + iniFilePath + ")... We use default setting", e);
        }

        return properties;
    }

    //
    // getProperty
    //

    // String
    public static String getProperty(Properties properties, String propertyName, String defaultValue) {
        if (properties == null) {
            return defaultValue;
        }

        String propertyValue = properties.getProperty(propertyName, defaultValue);
        log.info(String.format("%-25s --> %s", propertyName, propertyValue));

        return propertyValue;
    }
}
