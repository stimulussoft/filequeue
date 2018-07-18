package com.stimulussoft.filequeue.processor;

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/**
 * Implement this interface to consume items on the queue. File Queue will call the consume method each time an item
 * is available for processing.
 *
 * @author Jamie Band (Stimulus Software)
 * @author Valentin Popov (Stimulus Software)
 */

public interface Consumer<T> {

    /**
     * Result of consumption.
     *
     **/

    enum Result {

        /**
         * Processing of item was successful. Do not requeue.
         **/

        SUCCESS, /* process was successful */

        /**
         * Processing of item failed, however retry later.
         **/

        FAIL_REQUEUE,  /* process failed, but must be requeued */

        /**
         * Processing of item failed permanently. No retry.
         **/

        FAIL_NOQUEUE /* process failed, don't requeue */
    }

    /**
     * Consume the given item. This callback is called by FileQueue when an item is available for processing.
     *
     * @param item to handle.
     * @return {@code SUCCESS} if the item was processed successfully and shall be removed from the filequeue.
     * @throws InterruptedException if thread was interrupted due to shutdown
     */

    Result consume(T item) throws InterruptedException;

}