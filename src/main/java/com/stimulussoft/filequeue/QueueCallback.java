package com.stimulussoft.filequeue;

/**
 * Implement this interface to receive notification when an item is just about to be queued. It may, for example, be
 * used to cache files on disk at the point just before an item is submitted on the queue.
 *
 * @author Jamie Band (Stimulus Software)
 * @author Valentin Popov (Stimulus Software)
 */


public interface QueueCallback<T> {

    /**
     * Called when a slot is available in the queue for processing.
     * @param fileQueueItem the item subject to processing
     * @throws Exception thrown if something goes wrong, for example, run out of diskspace.
     */

    void availableSlot(final T fileQueueItem) throws Exception;

}
