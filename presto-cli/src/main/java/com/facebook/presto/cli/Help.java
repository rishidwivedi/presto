/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cli;

public final class Help
{
    private Help() {}

    public static String getHelpText()
    {
        return "" +
                "Supported commands:\n" +
                "QUIT\n" +
                "EXPLAIN [FORMAT {TEXT | GRAPHVIZ}] <query>\n" +
                "DESCRIBE <table>\n" +
                "SHOW COLUMNS FROM <table>\n" +
                "SHOW FUNCTIONS\n" +
                "SHOW CATALOGS\n" +
                "SHOW SCHEMAS\n" +
                "SHOW TABLES [LIKE <pattern>]\n" +
                "SHOW PARTITIONS FROM <table> [WHERE ...] [ORDER BY ...] [LIMIT n]\n" +
                "USE CATALOG <catalog>\n" +
                "USE SCHEMA <schema>\n" +
                "";
    }
}
