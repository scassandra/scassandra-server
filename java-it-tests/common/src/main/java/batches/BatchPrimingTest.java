package batches;/*
 * Copyright (C) 2014 Christopher Batey
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

import com.google.common.collect.Lists;
import common.*;
import org.junit.Test;
import org.scassandra.http.client.BatchType;
import org.scassandra.http.client.PrimingRequest;

import java.util.Collections;

import static org.junit.Assert.*;

abstract public class BatchPrimingTest extends AbstractScassandraTest {

    public BatchPrimingTest(CassandraExecutor cassandraExecutor) {
        super(cassandraExecutor);
    }

    @Test
    public void executeLoggedBatch() {
        CassandraResult result = cassandra().executeBatch(Lists.newArrayList(
                        new CassandraQuery("select * from blah"),
                        new CassandraQuery("select * from blah2")
                ), BatchType.UNLOGGED
        );

        assertEquals(PrimingRequest.Result.success, result.status().getResult());
        assertEquals(Collections.emptyList(), result.rows());
    }
}
