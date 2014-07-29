package tnode;

import static common.Common.*;

import java.io.IOException;

import main.HBShell;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

import exception.HBSException;
import task.TaskBase;
import task.TaskBase.Level;

public class TNodeFamilyFileData extends TNodeFamily {
    private static final long MAX_FBLOCK_COUNT_IN_ONE_ROW = 400;

    private final HTable  table;
    private final Long    firstFileDataTimestamp;
    private final Integer firstFileDataValuelength;
    private final byte[]  firstFileDataBValue;

    private final long minIndex;
    private final long maxIndex;

    private final TNodeFamily familyNode;

    public TNodeFamilyFileData(TaskBase task, TNodeRow parent, String family, HTable table, String firstFileDataQualifier, Long firstFileDataTimestamp, Integer firstFileDataValuelength, byte[] firstFileDataBValue, TNodeFamily familyNode, boolean toOutput)
    throws HBSException {
        super(task, parent, family, null, null, null, toOutput);

        this.table                    = table;
        this.firstFileDataTimestamp   = firstFileDataTimestamp;
        this.firstFileDataValuelength = firstFileDataValuelength;
        this.firstFileDataBValue      = firstFileDataBValue;

        this.minIndex = fileDataQualifierIndex(firstFileDataQualifier);
        this.maxIndex = minIndex + MAX_FBLOCK_COUNT_IN_ONE_ROW - 1;

        this.familyNode = familyNode;
    }

    @Override
    public void output()
    throws IOException, HBSException {
        // this file data family has other non-file-data
        if ((familyNode != null) && (familyNode.outputted)) {
            return;
        }

        super.output();
    }

    @Override
    protected void travelChildren()
    throws IOException, HBSException {
        // find last file data qualifier
        long firstIndex = minIndex;
        long lastIndex  = searchLastFileDataQualifier(minIndex, maxIndex, maxIndex * 2 - minIndex);

        if (travelQuickly()) {
            // no qualifier and value filter, show only the first and last file data qualifier

            // first file data qualifier
            new TNodeQualifier(task, this, fileDataQualifier(firstIndex), firstFileDataTimestamp, firstFileDataValuelength, firstFileDataBValue, toOutput).handle();

            if (lastIndex != firstIndex) {
                if (lastIndex > firstIndex + 1) {
                    new TNodeQualifierOmit(task, this, firstIndex, lastIndex, toOutput).handle();
                }

                // last file data qualifier
                Get    get                   = new Get(str2bytes(parent.name));
                String lastFileDataQualifier = fileDataQualifier(lastIndex);
                get.addColumn(str2bytes(name), str2bytes(lastFileDataQualifier));

                Result result = table.get(get);
                byte[] bValue = result.getValue(str2bytes(name), str2bytes(lastFileDataQualifier));

                Long    timestamp   = HBShell.showtimestamp ? result.raw()[0].getTimestamp() : null;
                Integer valuelength = HBShell.showvaluelength ? bValue.length : null;

                new TNodeQualifier(task, this, lastFileDataQualifier, timestamp, valuelength, bValue, toOutput).handle();
            }
        } else {
            // qualifier or value filter exists, so filter all
            for (long i = firstIndex; i <= lastIndex; i++) {
                Get get = new Get(str2bytes(parent.name));

                // set filter list
                FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

                // filter qualifier
                CompareFilter qualifierPatternFilter = getQualifierPatternFilter();

                if (qualifierPatternFilter != null) {
                    filterList.addFilter(qualifierPatternFilter);
                }

                // filter value
                CompareFilter valuePatternFilter = getValuePatternFilter();

                if (valuePatternFilter != null) {
                    filterList.addFilter(valuePatternFilter);
                }

                get.setFilter(filterList);

                // file data qualifier i only
                String fileDataQualifierI = fileDataQualifier(i);
                get.addColumn(str2bytes(name), str2bytes(fileDataQualifierI));

                // get result
                Result result = table.get(get);

                if (!result.isEmpty()) {
                    byte[] bValue = result.getValue(str2bytes(name), str2bytes(fileDataQualifierI));

                    Long    timestamp   = HBShell.showtimestamp ? result.raw()[0].getTimestamp() : null;
                    Integer valuelength = HBShell.showvaluelength ? bValue.length : null;

                    new TNodeQualifier(task, this, fileDataQualifierI, timestamp, valuelength, bValue, toOutput).handle();
                }
            }
        }
    }

    private boolean travelQuickly() {
        if (task.isHandleAll()) {
            return false;
        }

        return (task.levelParam.get(Level.QUALIFIER) == null) && (task.levelParam.get(Level.VALUE) == null) && (task.levelParam.get(Level.OTHER) == null);
    }

    // half search the last file data qualifier
    private long searchLastFileDataQualifier(long min, long lastIndex, long max)
    throws IOException {
        Get get = new Get(str2bytes(parent.name));
        get.addColumn(str2bytes(name), str2bytes(fileDataQualifier(lastIndex)));

        if (table.exists(get)) {
            if (lastIndex == maxIndex || lastIndex == max - 1) {
                return lastIndex;           // last file data qualifier(lastIndex) found
            }

            return searchLastFileDataQualifier(lastIndex, (lastIndex + max) / 2, max);
        }

        if (lastIndex == min + 1) {
            return min;                 // last file data qualifier(min) found
        }

        return searchLastFileDataQualifier(min, (min + lastIndex) / 2, lastIndex);
    }

    private static String fileDataQualifier(long index) {
        return "f" + index;
    }

    private static long fileDataQualifierIndex(String fileDataQualifier) {
        return Long.valueOf(fileDataQualifier.substring(1));
    }
}
