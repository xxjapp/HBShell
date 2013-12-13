package tnode;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Scanner;

import exception.HBSException;
import main.HBShell;
import task.TaskBase;
import task.TaskBase.Level;
import utils.Utils;

public class TNodeValue extends TNodeBase {
    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat(HBShell.format_timestamp, Locale.US);

    private final String timestamp;

    public TNodeValue(TaskBase task, TNodeQualifier parent, Long timestamp, byte[] bValue, boolean toOutput) {
        super(task, parent, valueString(bValue), Level.VALUE, toOutput);

        this.timestamp = timestampString(timestamp);
    }

    @Override
    protected String formatString() {
        if (HBShell.showtimestamp) {
            return parent.formatString() + " = %s " + HBShell.format_value;
        } else {
            return parent.formatString() + " = " + HBShell.format_value;
        }
    }

    @Override
    public void output()
    throws IOException, HBSException {
        if (!otherFilterPassed()) {
            return;
        }

        HBShell.increaseCount(HBShell.QUALIFIER);
        HBShell.increaseCount(HBShell.VALUE);

        if (toOutput) {
            parent.parent.output();

            if (HBShell.showtimestamp) {
                log.info(String.format(formatString(), parent.name, timestamp, name));
            } else {
                log.info(String.format(formatString(), parent.name, name));
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
