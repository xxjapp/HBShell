package utils;

import static org.apache.commons.io.IOUtils.closeQuietly;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;

public class PropertiesHelper {
    private static final Log log = RootLog.getLog();

    //
    // getProperties
    //

    public static Properties getProperties(String iniFilePath) {
        log.info(iniFilePath);
        return getPropertiesBase(iniFilePath);
    }

    public static Properties getPropertiesBase(String iniFilePath) {
        Properties      properties = new Properties();
        FileInputStream in         = null;

        try {
            in = new FileInputStream(iniFilePath);
            properties.load(in);
        } catch (IOException e) {
            log.error(null, e);
        } finally {
            closeQuietly(in);
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
        log.info(String.format("%30s --> %s", propertyName, propertyValue));

        return propertyValue;
    }

    // boolean
    public static boolean getProperty(Properties properties, String propertyName, boolean defaultValue) {
        String propertyValue = getProperty(properties, propertyName, String.valueOf(defaultValue));

        try {
            return Boolean.valueOf(propertyValue);
        } catch (NumberFormatException e) {
            log.warn(e);
            log.warn(String.format("%30s --> %s (Exception occured, reset to default)", propertyName, String.valueOf(defaultValue)));

            return defaultValue;
        }
    }

    // long
    public static long getProperty(Properties properties, String propertyName, long defaultValue) {
        String propertyValue = getProperty(properties, propertyName, String.valueOf(defaultValue));

        try {
            return Long.valueOf(propertyValue);
        } catch (NumberFormatException e) {
            log.warn(e);
            log.warn(String.format("%30s --> %s (Exception occured, reset to default)", propertyName, String.valueOf(defaultValue)));

            return defaultValue;
        }
    }

    // List<String>
    public static List<String> getProperty(Properties properties, String propertyName, List<String> defaultValue) {
        String propertyValue = getProperty(properties, propertyName, String.valueOf(defaultValue));

        try {
            return Arrays.asList(propertyValue.split("\\s*,\\s*"));
        } catch (NumberFormatException e) {
            log.warn(e);
            log.warn(String.format("%30s --> %s (Exception occured, reset to default)", propertyName, String.valueOf(defaultValue)));

            return defaultValue;
        }
    }
}
