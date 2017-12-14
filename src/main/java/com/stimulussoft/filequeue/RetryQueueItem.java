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
 * @author Jamie Band (adaptation to MVStore, added multithreaded consumer support & retry delay option)
 * @author Valentin Popov
 * Thanks for Martin Grotze for his original work on Persistent Queue
 */

package com.stimulussoft.filequeue;

import java.util.Date;

/**
 * Your custom QueueItem should extends this abstract class.
 * It will be serialized/deserialized using Jackson JSON.
 *
 * @author Valentin Popov
 * @author Jamie Band
 */

public abstract class RetryQueueItem implements FileQueueItem {

    protected int retryCount;
    protected Date tryDate;

    public Date getTryDate() {
        return tryDate;
    }

    public void setTryDate(Date date) {
        this.tryDate = date;
    }

    public int getTryCount() {
        return retryCount;
    }

    public void setTryCount(int tryCound) {
        this.retryCount = tryCound;
    }

    public void incTryCount() {
        this.retryCount++;
    }

    public abstract String toString();
}
 