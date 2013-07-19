package common;

import java.lang.reflect.Field;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;

public class LogHelper {
    public static Log getLog() {
        StackTraceElement callerInfo = Common.getCallerInfo();
        String            className  = callerInfo.getClassName();

        return LogFactory.getLog(className);
    }

    public static void logFields(Object object) {
        logFields(object, Level.INFO);
    }

    public static void logFields(Object object, Level level) {
        Log log = getLog();

        if (level == Level.INFO) {
            log.info(Common.getCallerInfo());
        } else {
            log.debug(Common.getCallerInfo());
        }

        Field[] fields = object.getClass().getFields();

        for (Field field : fields) {
            String name  = field.getName();
            Object value = null;

            try {
                value = field.get(object);
            } catch (Exception e) {
                log.error(null, e);
            }

            if (level == Level.INFO) {
                log.info(name + " = " + value);
            } else {
                log.debug(name + " = " + value);
            }
        }
    }
}
