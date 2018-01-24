/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.logging;

import java.util.logging.Level;

public class Logger {

    public static final ILogger DEFAULT = new AbstractLogger() {
        @Override
        public void log(Level level, String message) {
//            System.out.println(level + ": " + message);
        }

        @Override
        public void log(Level level, String message, Throwable thrown) {
//            System.out.println(level + ": " + message + " " + thrown);
        }

        @Override public Level getLevel() {
            return Level.INFO;
        }

        @Override
        public boolean isLoggable(Level level) {
            return true;
        }
    };
}
