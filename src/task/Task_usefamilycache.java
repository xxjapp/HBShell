package task;

import utils.Utils;

public class Task_usefamilycache extends TaskStatusBase {
    public Task_usefamilycache() {
        super("usefamilycache");
    }

    @Override
    protected void setStatus(String status) {
        super.setStatus(status);

        if (!Boolean.valueOf(status)) {
            Utils.clearFamilyCache();
        }
    }
}
