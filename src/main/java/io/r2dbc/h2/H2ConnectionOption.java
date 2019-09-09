/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.h2;

/**
 * Collection of H2 configuration options.
 */
public enum H2ConnectionOption {

    /**
     * FILE|SOCKET|NO
     *
     * @see <a href="http://www.h2database.com/html/features.html#database_file_locking" target="_top">Database File Locking</a>
     */
    FILE_LOCK,

    /**
     * TRUE|FALSE
     *
     * @see <a href="http://www.h2database.com/html/features.html#database_only_if_exists" target="_top">Opening a Database Only If It Already Exists</a>
     */
    IFEXISTS,

    /**
     * Seconds to stay open or {@literal -1} to to keep in-memory DB open as long as the virtual machine is alive.
     *
     * @see <a href="http://www.h2database.com/html/features.html#closing_a_database" target="_top">Delayed Database Closing</a>
     */
    DB_CLOSE_DELAY,

    /**
     * TRUE|FALSE
     *
     * @see <a href="http://www.h2database.com/html/features.html#do_not_close_on_exit" target="_top">Don't Close a Database when the VM Exits</a>
     */
    DB_CLOSE_ON_EXIT,

    /**
     * DML or DDL commands on startup, use "\\;" to chain multiple commands
     *
     * @see <a href="http://www.h2database.com/html/features.html#execute_sql_on_connection" target="_top">Execute SQL on Connection</a>
     */
    INIT,

    /**
     * 0..3 (0=OFF, 1=ERROR, 2=INFO, 3=DEBUG)
     *
     * @see <a href="http://www.h2database.com/html/features.html#trace_options" target="_top">Using the Trace Options</a>
     */
    TRACE_LEVEL_FILE,

    /**
     *  Megabytes (to override the 16mb default, e.g. 64)
     *
     * @see <a href="http://www.h2database.com/html/features.html#trace_options" target="_top">Using the Trace Options</a>
     */
    TRACE_MAX_FILE_SIZE,

    /**
     * 0..3 (0=OFF, 1=ERROR, 2=INFO, 3=DEBUG)
     *
     * @see <a href="http://www.h2database.com/html/features.html#other_settings" target="_top">Changing Other Settings</a>
     */
    TRACE_LEVEL_SYSTEM_OUT,

    /**
     *
     */
    LOG,

    /**
     * TRUE|FALSE
     *
     * @see <a href="http://www.h2database.com/html/features.html#ignore_unknown_settings" target="_top">Ignore Unknown Settings</a>
     */
    IGNORE_UNKNOWN_SETTINGS,

    /**
     * r|rw|rws|rwd (r=read, rw=read/write)
     *
     * @see <a href="http://www.h2database.com/html/features.html#custom_access_mode" target="_top">Custom File Access Mode</a>
     */
    ACCESS_MODE_DATA,

    /**
     * DB2|Derby|HSQLDB|MSSQLServer|MySQL|Oracle|PostgreSQL|Ignite
     *
     * @see <a href="http://www.h2database.com/html/features.html#compatibility" target="_top">Compatibility Mode</a>
     */
    MODE,

    /**
     *  TRUE|FALSE
     *
     * @see <a href="http://www.h2database.com/html/features.html#auto_mixed_mode" target="_top">Auto Mixed Mode</a>
     */
    AUTO_SERVER,

    /**
     * A port number
     *
     * @see <a href="http://www.h2database.com/html/features.html#auto_mixed_mode" target="_top">Auto Mixed Mode</a>
     */
    AUTO_SERVER_PORT,

    /**
     * Bytes (e.g. 512)
     *
     * @see <a href="http://www.h2database.com/html/features.html#page_size" target="_top">Page Size</a>
     */
    PAGE_SIZE,

    /**
     * Number of threads (e.g. 4)
     *
     * @see <a href="http://www.h2database.com/html/features.html#multiple_connections" target="_top">Multiple Connections</a>
     */
    MULTI_THREADED,

    /**
     * TQ|SOFT_LRU
     *
     * @see <a href="http://www.h2database.com/html/features.html#cache_settings" target="_top">Cache Settings</a>
     */
    CACHE_TYPE,

    /**
     * TRUE|FALSE
     *
     * @see <a href="http://www.h2database.com/html/advanced.html#password_hash" target="_top">Password Hash</a>
     */
    PASSWORD_HASH;

    public String getKey() {
        return this.name();
    }
}
