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
 */

package com.stimulussoft.util;

import java.util.concurrent.Semaphore;

public class AdjustableSemaphore extends Semaphore {

    int numberOfPermits = 0;

    public AdjustableSemaphore() {
        super(0, true);
    }

    public synchronized void setMaxPermits(int desiredPermits) {
        if (desiredPermits > numberOfPermits)
            release(desiredPermits - numberOfPermits);
        else if (desiredPermits < numberOfPermits)
            reducePermits(numberOfPermits - desiredPermits);
        numberOfPermits = desiredPermits;
    }
};