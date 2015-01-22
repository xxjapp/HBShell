package tnode;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import main.HBShell;
import task.TaskBase;
import task.TaskBase.Level;
import utils.Utils;
import exception.HBSException;

public class TNodeFamily extends TNodeBase {
    private final Map<String, Long>            timestampMap;
    private final Map<String, Integer>         valuelengthMap;
    private final NavigableMap<byte[], byte[]> familyMap;

    public TNodeFamily(TaskBase task, TNodeRow parent, String family, Map<String, Long> timestampMap, Map<String, Integer> valuelengthMap, NavigableMap<byte[], byte[]> familyMap, boolean toOutput)
    throws HBSException {
        super(task, parent, family, Level.FAMILY, toOutput);

        this.timestampMap   = timestampMap;
        this.valuelengthMap = valuelengthMap;
        this.familyMap      = familyMap;
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
        for (byte[] bQualifier : familyMap.keySet()) {
            String qualifier = Utils.bytes2str(bQualifier);

            Long    timestamp   = HBShell.showtimestamp ? timestampMap.get(qualifier) : null;
            Integer valuelength = HBShell.showvaluelength ? valuelengthMap.get(qualifier) : null;

            new TNodeQualifier(task, this, qualifier, timestamp, valuelength, familyMap.get(bQualifier), toOutput).handle();
        }
    }

    public static boolean isFileDataFamily(String family) {
        return family.equals("file") || family.equals("tmp") || family.equals("textmemo");
    }
}
