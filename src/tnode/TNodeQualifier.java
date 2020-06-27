package tnode;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;

import exception.HBSException;
import main.HBShell;
import task.TaskBase;
import task.TaskBase.Level;

public class TNodeQualifier extends TNodeBase {
    private final KeyValue kv;

    public TNodeQualifier(TaskBase task, TNodeFamily parent, String qualifier, KeyValue kv, boolean toOutput)
    throws HBSException {
        super(task, parent, qualifier, Level.QUALIFIER, toOutput);
        this.kv = kv;
    }

    @Override
    protected String formatString() {
        return HBShell.format_qualifier;
    }

    @Override
    public void output()
    throws IOException, HBSException {
        if (!outputted) {
            HBShell.increaseCount(HBShell.QUALIFIER);
        }

        super.output();
    }

    @Override
    protected void travelChildren()
    throws IOException, HBSException {
        new TNodeValue(task, this, kv, toOutput).handle();
    }
}
