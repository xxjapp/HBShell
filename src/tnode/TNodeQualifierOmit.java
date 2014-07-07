package tnode;

import java.io.IOException;

import exception.HBSException;

import main.HBShell;

import task.TaskBase;
import task.TaskBase.Level;

public class TNodeQualifierOmit extends TNodeBase {
    private static final String NAME = "...";

    private final long firstIndex;
    private final long lastIndex;

    public TNodeQualifierOmit(TaskBase task, TNodeFamily parent, long firstIndex, long lastIndex, boolean toOutput)
    throws HBSException {
        super(task, parent, NAME, Level.QUALIFIER, toOutput);

        this.firstIndex = firstIndex;
        this.lastIndex  = lastIndex;
    }

    @Override
    protected String formatString() {
        return HBShell.format_qualifierOmit;
    }

    @Override
    public void handle()
    throws IOException, HBSException {
        long count = lastIndex - firstIndex - 1;

        HBShell.increaseCount(HBShell.QUALIFIER, count);
        HBShell.increaseCount(HBShell.VALUE, count);

        output();
    }

    @Override
    protected void travelChildren() {
        // do nothing
    }
}
