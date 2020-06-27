package tnode;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;

import exception.HBSException;
import exception.HBSExceptionRowLimitReached;
import main.HBShell;
import task.TaskBase;
import task.TaskBase.Level;
import utils.Utils;

public class TNodeTable extends TNodeBase {
    private HTableInterface table = null;

    public TNodeTable(TaskBase task, TNodeDatabase parent, String name, boolean toOutput)
    throws HBSException {
        super(task, parent, name, Level.TABLE, toOutput);
    }

    @Override
    protected String formatString() {
        return HBShell.format_table;
    }

    @Override
    public void output()
    throws IOException, HBSException {
        if (!outputted) {
            HBShell.increaseCount(HBShell.TABLE);
        }

        super.output();
    }

    @Override
    protected void travelChildren()
    throws IOException, HBSException {
        // set filter list
        FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

        addRowFilterToFilterList(filterList);
        filterList.addFilter(new FirstKeyOnlyFilter());

        Scan scan = new Scan();
        scan.setFilter(filterList);

        this.table = Utils.getTable(name);

        try {
            try (ResultScanner resultScanner = table.getScanner(scan)) {
                for (Result firstKVResult : resultScanner) {
                    new TNodeRow(task, this, table, firstKVResult.raw()[0], toOutput).handle();

                    // check row limit
                    if (HBShell.getCount(HBShell.ROW) >= task.getRowLimit()) {
                        throw new HBSExceptionRowLimitReached();
                    }
                }
            }
        } finally {
            table.close();
            table = null;
        }
    }

    private void addRowFilterToFilterList(FilterList filterList) {
        Object pattern = task.levelParam.get(Level.ROW);

        if (pattern != null) {
            RegexStringComparator comparator = new RegexStringComparator(pattern.toString());
            filterList.addFilter(new RowFilter(CompareOp.EQUAL, comparator));
        }
    }

    public HTableInterface getTable()
    throws IOException {
        if (table != null) {
            return table;
        }

        return Utils.getTable(name);
    }

    public void closeTable(HTableInterface table)
    throws IOException {
        if (table != this.table) {
            table.close();
        }
    }
}
