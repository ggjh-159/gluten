/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.table.runtime.stream.custom;

import org.apache.gluten.table.runtime.stream.common.GlutenStreamingTestBase;

import org.apache.flink.table.api.TableException;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Plan-time guards of the velox group aggregate. Global aggregation (no GROUP BY keys) and state
 * TTL are not supported by the native keyed state yet, so the planner must reject them with a clear
 * TableException instead of failing later at task start with a JNI error (empty key channel list)
 * or silently ignoring the setting (state growing without bound). The native Flink planner accepts
 * both queries, so observing the exceptions proves the gluten translation is active.
 */
class GroupAggregateGuardTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testGlobalAggregateRejectedAtPlanTime() {
    List<Row> rows = Arrays.asList(Row.of(1L, 10L), Row.of(2L, 20L));
    createSimpleBoundedValuesTable("globalAggTbl", "a bigint, c bigint", rows);
    assertThatThrownBy(() -> explainExecutionPlan("select count(*) from globalAggTbl"))
        .isInstanceOf(TableException.class)
        .hasMessageContaining("global aggregation");
  }

  @Test
  void testStateTtlRejectedAtPlanTime() {
    List<Row> rows = Arrays.asList(Row.of(1L, 10L), Row.of(1L, 20L), Row.of(2L, 30L));
    createSimpleBoundedValuesTable("ttlAggTbl", "a bigint, c bigint", rows);
    tEnv().getConfig().getConfiguration().setString("table.exec.state.ttl", "1 h");
    assertThatThrownBy(() -> explainExecutionPlan("select a, count(c) from ttlAggTbl group by a"))
        .isInstanceOf(TableException.class)
        .hasMessageContaining("state TTL");
  }
}
