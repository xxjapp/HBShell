package task;

public class Task_count extends TaskBase {
    @Override
    protected String description() {
        return "count number of tables, rows, families, qualifiers, or values at a specified level";
    }

    @Override
    protected String usage() {
        return "count [table_pattern [row_pattern [family_pattern [qualifier_pattern [value_pattern]]]]]";
    }

    @Override
    public String example() {
        return "count ^135530186920f18b9049b0a0743e86ac3185887c5d .";
    }

    @Override
    protected boolean checkArgNumber(int argNumber) {
        return 0 <= argNumber && argNumber <= 5;
    }

    @Override
    public Level getLevel() {
        if (levelParam.size() > 0) {
            return Level.values()[levelParam.size() - 1];
        }

        return Level.TABLE;
    }

    @Override
    public boolean isToOutput() {
        return false;
    }
}
