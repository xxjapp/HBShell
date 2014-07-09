package common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LogHelper {
    public static Log getLog() {
        StackTraceElement callerInfo = Common.getCallerInfo();
        String            className  = callerInfo.getClassName();

        return LogFactory.getLog(className);
    }
}
