package tnode;

import static common.Common.str2bytes;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

import exception.HBSException;
import task.TaskBase;
import task.TaskBase.Level;
import utils.Utils;

public class TNodeFamilyFileData extends TNodeFamily {
    private static final long MAX_FBLOCK_COUNT_IN_ONE_ROW = 400;

    private final HTableInterface table;
    private final long            startIndex;
    private final TNodeFamily     familyNode;
    private final KeyValue        firstKv;

    public TNodeFamilyFileData(TaskBase task, TNodeRow parent, String family, HTableInterface table, KeyValue firstKv, TNodeFamily familyNode, boolean toOutput)
    throws HBSException {
        super(task, parent, family, null, 0, toOutput);

        this.table      = table;
        this.startIndex = fileDataQualifierIndex(Utils.bytes2str(firstKv.getQualifier()));
        this.familyNode = familyNode;
        this.firstKv    = firstKv;
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
        long firstIndex = searchBoundaryFileDataQualifier(startIndex, startIndex - MAX_FBLOCK_COUNT_IN_ONE_ROW);
        long lastIndex  = searchBoundaryFileDataQualifier(startIndex, startIndex + MAX_FBLOCK_COUNT_IN_ONE_ROW);

        if (travelQuickly()) {
            // no qualifier and value filter, show only the first and last file data qualifier

            // first file data qualifier
            new TNodeQualifier(task, this, fileDataQualifier(firstIndex), firstKv, toOutput).handle();

            if (lastIndex != firstIndex) {
                if (lastIndex > firstIndex + 1) {
                    new TNodeQualifierOmit(task, this, firstIndex, lastIndex, toOutput).handle();
                }

                // last file data qualifier
                Get    get                   = new Get(str2bytes(parent.name));
                String lastFileDataQualifier = fileDataQualifier(lastIndex);
                get.addColumn(str2bytes(name), str2bytes(lastFileDataQualifier));

                Result result = table.get(get);
                new TNodeQualifier(task, this, lastFileDataQualifier, result.raw()[0], toOutput).handle();
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
                    new TNodeQualifier(task, this, fileDataQualifierI, result.raw()[0], toOutput).handle();
                }
            }
        }
    }

    // startIndex  qualifier should always be OK
    // maxEndIndex qualifier should always be NG
    private long searchBoundaryFileDataQualifier(long startIndex, long maxEndIndex)
    throws IOException {
        if (Math.abs(startIndex - maxEndIndex) == 1) {
            return startIndex;
        }

        long middleIndex = (startIndex + maxEndIndex) / 2;

        if (qualifierExist(middleIndex)) {
            return searchBoundaryFileDataQualifier(middleIndex, maxEndIndex);
        } else {
            return searchBoundaryFileDataQualifier(startIndex, middleIndex);
        }
    }

    private boolean qualifierExist(long index)
    throws IOException {
        Get get = new Get(str2bytes(parent.name));
        get.addColumn(str2bytes(name), str2bytes(fileDataQualifier(index)));
        return table.exists(get);
    }

    private boolean travelQuickly() {
        if (task.isHandleAll()) {
            return false;
        }

        return (task.levelParam.get(Level.QUALIFIER) == null) && (task.levelParam.get(Level.VALUE) == null) && (task.levelParam.get(Level.OTHER) == null);
    }

    private static String fileDataQualifier(long index) {
        return "f" + index;
    }

    private static long fileDataQualifierIndex(String fileDataQualifier) {
        return Long.valueOf(fileDataQualifier.substring(1));
    }
}
