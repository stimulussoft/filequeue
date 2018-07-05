package com.stimulussoft.filequeue;

import java.io.IOException;

public interface QueueCallback<T> {

    void availableSlot(final T fileQueueItem) throws Exception;

}
