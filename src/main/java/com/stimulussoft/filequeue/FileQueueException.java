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
package com.stimulussoft.filequeue;

import org.slf4j.Logger;

import java.io.Serializable;

/**
 * General file queue exception
 *
 * @author Valentin Popov (Stimulus Software)
 * @author Jamie Band (Stimulus Software)
 */

public class FileQueueException extends Exception  {


    public FileQueueException(String message, Logger logger) {
        super(message);
        logger.error(message);
    }

    public FileQueueException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileQueueException(String message, Throwable cause, Logger logger) {
        super(message, cause);
        logger.error(message);
    }

    public FileQueueException(String message) {
        super(message);
    }

    @Override
    public String getMessage() {
        return super.getMessage();
    }

}
