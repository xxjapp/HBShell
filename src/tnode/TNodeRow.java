package tnode;

import static common.Common.str2bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import exception.HBSException;
import main.HBShell;
import task.TaskBase;
import task.TaskBase.Level;
import utils.Utils;

public class TNodeRow extends TNodeBase {
    public final HTableInterface table;
    private final KeyValue       firstKv;

    private List<String> families         = null;
    private List<String> fileDataFamilies = null;

    public TNodeRow(TaskBase task, TNodeTable parent, HTableInterface table, KeyValue firstKv, boolean toOutput)
    throws HBSException {
        super(task, parent, Utils.bytes2str(firstKv.getRow()), Level.ROW, toOutput);

        this.table   = table;
        this.firstKv = firstKv;
    }

    @Override
    protected String formatString() {
        return HBShell.format_row;
    }

    @Override
    public void output()
    throws IOException, HBSException {
        if (!outputted) {
            HBShell.increaseCount(HBShell.ROW);
        }

        super.output();
    }

    @Override
    protected void travelChildren()
    throws IOException, HBSException {
        // get filtered families and fileDataFamilies
        List<String> allFamilies = Utils.getFamilies(table);
        this.families         = new ArrayList<>();
        this.fileDataFamilies = new ArrayList<>();

        for (String family : allFamilies) {
            if (task.isMatch(Level.FAMILY, family)) {
                families.add(family);

                if (TNodeFamily.isFileDataFamily(family)) {
                    fileDataFamilies.add(family);
                }
            }
        }

        // travel non-file-data
        // if file-data is in the same family, they will also be travelled
        travelChildrenNonFileData();

        // travel remained file-data
        for (String family : fileDataFamilies) {
            TNodeFamilyFileData familyFileData = getFamilyFileData(family, null);

            if (familyFileData != null) {
                familyFileData.handle();
            }
        }
    }

    private void travelChildrenNonFileData()
    throws IOException, HBSException {
        Get get = new Get(str2bytes(name));

        // filter family
        for (String family : families) {
            get.addFamily(str2bytes(family));
        }

        FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

        // filter file data qualifier (excluded)
        filterList.addFilter(getFileDataQualifierFilter(false));

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

        // get result
        Result result = table.get(get);

        if (result.isEmpty()) {
            return;
        }

        KeyValue[] kvs = result.raw();

        for (int kvIndex = 0; kvIndex < kvs.length; kvIndex++) {
            KeyValue kv     = kvs[kvIndex];
            String   family = Utils.bytes2str(kv.getFamily());

            // if no result, hbase will get all families
            // so filter family again
            if (!families.contains(family)) {
                continue;
            }

            TNodeFamily familyNode = new TNodeFamily(task, this, family, kvs, kvIndex, toOutput);

            familyNode.handle();

            if (TNodeFamily.isFileDataFamily(family)) {
                TNodeFamilyFileData familyFileData = getFamilyFileData(family, familyNode);

                if (familyFileData != null) {
                    familyFileData.handle();
                }

                fileDataFamilies.remove(family);
            }

            kvIndex = familyNode.getLastKvIndex();
        }
    }

    public TNodeFamilyFileData getFamilyFileData(String family, TNodeFamily familyNode)
    throws IOException, HBSException {
        if (!HBShell.travelRowFBlockFamilies) {
            return null;
        }

        // if firstKVResult is file data, then use it directly instead of getting it once more
        TNodeFamilyFileData familyFileData = getFamilyFileDataUsingFirstKVResult(family, familyNode);

        if (familyFileData != null) {
            return familyFileData;
        }

        Get get = new Get(str2bytes(name));

        // filter family
        get.addFamily(str2bytes(family));

        FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

        // filter file data qualifier (included)
        // this filter must be previous to FirstKeyOnlyFilter
        // the order is important
        filterList.addFilter(getFileDataQualifierFilter(true));

        // do not filter qualifier & value, filter them in family level

        // NOTE:
        //
        // f800
        // f801
        // f802
        // ...
        // f1000	<= this will be the first qualifier!
        // f1001
        // f1002
        // ...
        // f1199
        //
        // get only first kv
        filterList.addFilter(new FirstKeyOnlyFilter());

        get.setFilter(filterList);

        // get result
        Result result = table.get(get);

        if (!result.isEmpty()) {
            return new TNodeFamilyFileData(task, this, family, table, result.raw()[0], familyNode, toOutput);
        }

        return null;
    }

    private TNodeFamilyFileData getFamilyFileDataUsingFirstKVResult(String family, TNodeFamily familyNode)
    throws HBSException {
        String qualifier = Utils.bytes2str(firstKv.getQualifier());

        if (!Utils.isMatch(qualifier, FILE_DATA_QUALIFIER_PATTERN)) {
            return null;
        }

        return new TNodeFamilyFileData(task, this, family, table, firstKv, familyNode, toOutput);
    }
}
