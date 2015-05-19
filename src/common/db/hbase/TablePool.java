package common.db.hbase;

import static utils.Utils.conf;

import org.apache.hadoop.hbase.client.HTablePool;

public class TablePool {
    // properties

    private final HTablePool tablePool;

    // singleton
    // SEE: http://en.wikipedia.org/wiki/Singleton_pattern#The_solution_of_Bill_Pugh

    private static class SingletonHolder {
        private static final TablePool INSTANCE = new TablePool();
    }

    public static HTablePool inst() {
        return SingletonHolder.INSTANCE.tablePool;
    }

    // constructors

    private TablePool() {
        this.tablePool = new HTablePool(conf(), Integer.MAX_VALUE);
    }
}
