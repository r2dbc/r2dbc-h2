/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.h2.client;

import org.h2.value.ValueInt;
import org.junit.jupiter.api.Test;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

final class ClientTest {

    @Test
    void parseMultilineSelect() {
        Vector<String> sqls = new Vector<String>();
        sqls.add("SELECT 1 FROM DUAL"); 
        sqls.add("SELECT\n\t1\n\tFROM\n\tDUAL"); 
        sqls.add("\nSELECT * FROM DUAL\n");
        sqls.add(" SELECT\n* FROM DUAL");
        sqls.add("  SELECT * FROM DUAL");
        for(String sql : sqls) {
          assertTrue(Client.SELECT.matcher(sql).matches());
        }
    }

    @Test
    void parseMultilineInsert() {
        Vector<String> sqls = new Vector<String>();
        sqls.add("INSERT INTO table (id, name, age)\nVALUES(1, 'billy', 28)");
        sqls.add("INSERT INTO TABLE (id, name, age) VALUES(1,2,3)"); 
        sqls.add("INSERT\n\tINTO TABLE (id, name, age)\n\t VALUES(1,2,3)"); 
        sqls.add(" INSERT\n\tINTO TABLE\n\t(id, anme)\n\tVALUES(1, 'bob')"); 
        sqls.add("\nINSERT\n\tINTO TABLE\n\t(id, anme)\n\tVALUES(1, 'bob')"); 
        sqls.add(" \n INSERT\n\tINTO TABLE\n\t(id, anme)\n\tVALUES(1, 'bob')"); 
        for(String sql : sqls) {
          assertTrue(Client.INSERT.matcher(sql).matches());
        }
    }
}
