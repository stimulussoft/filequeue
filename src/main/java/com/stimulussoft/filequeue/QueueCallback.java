package com.stimulussoft.filequeue;

import java.io.IOException;

public interface QueueCallback {

    void availableSlot(final FileQueueItem fileQueueItem) throws IOException;

}
