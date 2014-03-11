package tnode;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;

import main.HBShell;
import task.TaskBase;
import task.TaskBase.Level;
import utils.Utils;

public class TNodeValue extends TNodeBase {
    private static final SimpleDateFormat timestampFormat   = new SimpleDateFormat(HBShell.format_timestamp, Locale.US);
    private static final String           valuelengthFormat = HBShell.format_valuelength;

    private final byte[] bValue;
    private final String timestamp;
    private final String valuelength;

    public TNodeValue(TaskBase task, TNodeQualifier parent, Long timestamp, Integer valuelength, byte[] bValue, boolean toOutput) {
        super(task, parent, valueString(bValue), Level.VALUE, toOutput);

        this.bValue      = bValue;
        this.timestamp   = timestampString(timestamp);
        this.valuelength = valuelengthString(valuelength);
    }

    @Override
    protected String formatString() {
        String format = parent.formatString() + " = ";

        if (HBShell.showtimestamp) {
            format += "%s ";
        }

        if (HBShell.showvaluelength) {
            format += "%s ";
        }

        format += HBShell.format_value;

        return format;
    }

    @Override
    public void output()
    throws IOException {
        if (!otherFilterPassed()) {
            return;
        }

        HBShell.increaseCount(HBShell.QUALIFIER);
        HBShell.increaseCount(HBShell.VALUE);

        if (toOutput) {
            parent.parent.output();

            if (HBShell.showtimestamp && HBShell.showvaluelength) {
                log.info(String.format(formatString(), parent.name, timestamp, valuelength, name));
            } else if (HBShell.showtimestamp && !HBShell.showvaluelength) {
                log.info(String.format(formatString(), parent.name, timestamp, name));
            } else if (!HBShell.showtimestamp && HBShell.showvaluelength) {
                log.info(String.format(formatString(), parent.name, valuelength, name));
            } else {
                log.info(String.format(formatString(), parent.name, name));
            }

            if (task.outpuBinary()) {
                String valueFilePath = String.format("%s/%s/%s/%s/%s.txt", HBShell.binaryDataDir, parent.parent.parent.parent.name, parent.parent.parent.name, parent.parent.name, parent.name);
                FileUtils.writeByteArrayToFile(new File(valueFilePath), bValue);
            }
        }

        if (task != null) {
            task.notifyFound(this);
        }
    }

    @Override
    protected void travelChildren() {
        // no children
    }

    private static String timestampString(Long timestamp) {
        if (timestamp == null) {
            return null;
        }

        Date date = new Date(timestamp);
        return timestampFormat.format(date);
    }

    private static String valuelengthString(Integer valuelength) {
        if (valuelength == null) {
            return null;
        }

        return String.format(valuelengthFormat, valuelength.intValue());
    }

    private static String valueString(byte[] bValue) {
        if (bValue.length == 0) {
            return "";
        }

        String value = null;

        if (!Utils.isPrintableData(bValue, HBShell.maxPrintableDetectCnt)) {
            value = Utils.getHexStringBase(bValue, HBShell.maxHexStringLength.intValue(), true);
        } else {
            int length = (int)Math.min(bValue.length, HBShell.maxPrintableDetectCnt);
            value = Utils.bytes2str(bValue, 0, length);

            if (bValue.length > HBShell.maxPrintableDetectCnt) {
                value += " ...";
            }

            if (!HBShell.multiline) {
                // show only first line
                String firstLine = new Scanner(value).nextLine();

                if (firstLine.length() < value.length()) {
                    value = firstLine + " ...";
                }
            }
        }

        return value;
    }
}
