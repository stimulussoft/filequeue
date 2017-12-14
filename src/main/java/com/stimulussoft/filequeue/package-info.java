/**
 * The FileQueue project offers a light weight, high performance, simple, reliable and persistent queue for Java applications. All Producers and consumers are run within a single Java runtime. To provide persistence, FileQueue leverages the MVStore database engine from H2. Queue items are regular Java POJOs, serialized into Json using jackson.
 * To attain higher levels of performance, FileQueue will transfer queued items directly to consumers without hitting the database provided there are consumers available. If all consumers are busy, file queue will automatically persistent queued items to the database.
 * FileQueue also offers basic retry logic. It can be configured to redeliver items that could not be processed at a later stage.
 * <p>
 * Refer to <a href="https://github.com/stimulussoft/filequeue">https://github.com/stimulussoft/filequeue</a> for more information on usage.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with the License. You may obtain a copy of the License at ttp://www.apache.org/licenses/LICENSE-2.0.
 * <p>
 * FileQueue is copyright Stimulus Software, implemented by Valentin Popov and Jamie Band.
 *
 * @see com.stimulussoft.filequeue
 * @since 1.0
 */
package com.stimulussoft.filequeue;
