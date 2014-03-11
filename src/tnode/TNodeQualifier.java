package tnode;

import java.io.IOException;

import exception.HBSException;
import main.HBShell;
import task.TaskBase;
import task.TaskBase.Level;

public class TNodeQualifier extends TNodeBase {
    private final byte[]  bValue;
    private final Long    timestamp;
    private final Integer valuelength;

    public TNodeQualifier(TaskBase task, TNodeFamily parent, String qualifier, Long timestamp, Integer valuelength, byte[] bValue, boolean toOutput) {
        super(task, parent, qualifier, Level.QUALIFIER, toOutput);

        this.timestamp   = timestamp;
        this.valuelength = valuelength;
        this.bValue      = bValue;
    }

    @Override
    protected String formatString() {
        return HBShell.format_qualifier;
    }

    @Override
    public void output()
    throws IOException {
        if (!outputted) {
            HBShell.increaseCount(HBShell.QUALIFIER);
        }

        super.output();
    }

    @Override
    protected void travelChildren()
    throws IOException, HBSException {
        new TNodeValue(task, this, timestamp, valuelength, bValue, toOutput).handle();
    }
}
