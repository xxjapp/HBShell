package tnode;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;

import exception.HBSException;
import main.HBShell;
import task.TaskBase;
import task.TaskBase.Level;
import utils.Utils;

public class TNodeFamily extends TNodeBase {
    private final KeyValue[] kvs;
    private final int        initialKvIndex;

    private int lastKvIndex;

    public TNodeFamily(TaskBase task, TNodeRow parent, String family, KeyValue[] kvs, int initialKvIndex, boolean toOutput)
    throws HBSException {
        super(task, parent, family, Level.FAMILY, toOutput);

        this.kvs            = kvs;
        this.initialKvIndex = initialKvIndex;
        this.lastKvIndex    = initialKvIndex;
    }

    @Override
    protected String formatString() {
        return HBShell.format_family;
    }

    @Override
    public void output()
    throws IOException, HBSException {
        if (!outputted) {
            HBShell.increaseCount(HBShell.FAMILY);
        }

        super.output();
    }

    @Override
    protected void travelChildren()
    throws IOException, HBSException {
        for (int kvIndex = initialKvIndex; kvIndex < kvs.length; kvIndex++) {
            KeyValue kv     = kvs[kvIndex];
            String   family = Utils.bytes2str(kv.getFamily());

            // NOTE: last family in kvs will not hit this block
            if (!family.equals(this.name)) {
                return;
            }

            lastKvIndex = kvIndex;

            String qualifier = Utils.bytes2str(kv.getQualifier());
            new TNodeQualifier(task, this, qualifier, kv, toOutput).handle();
        }
    }

    public int getLastKvIndex() {
        return lastKvIndex;
    }

    public static boolean isFileDataFamily(String family) {
        return family.equals("file") || family.equals("tmp") || family.equals("textmemo");
    }
}
