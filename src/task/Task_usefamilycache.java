package task;

import java.io.IOException;

import utils.Utils;

public class Task_usefamilycache extends TaskStatusBase {
    public Task_usefamilycache() {
        super("usefamilycache");
    }

    @Override
    protected void setStatus(String status)
    throws IOException {
        super.setStatus(status);

        if (!Boolean.valueOf(status)) {
            Utils.clearFamilyCache();
        }
    }
}
