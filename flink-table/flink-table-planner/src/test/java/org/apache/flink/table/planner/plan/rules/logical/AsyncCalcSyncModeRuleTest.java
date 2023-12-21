/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.plan.rules.FlinkStreamRuleSets;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test for {@link AsyncCalcSyncModeRule}. */
public class AsyncCalcSyncModeRuleTest extends TableTestBase {

    private TableTestUtil util = streamTestUtil(TableConfig.getDefault());

    @BeforeEach
    public void setup() {
        FlinkChainedProgram programs = new FlinkChainedProgram<BatchOptimizeContext>();
        programs.addLast(
                "logical_rewrite",
                FlinkHepRuleSetProgramBuilder.newBuilder()
                        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
                        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                        .add(FlinkStreamRuleSets.LOGICAL_REWRITE())
                        .build());

        util.addTableSource(
                "MyTable",
                ImmutableList.<TypeInformation<?>>of(
                                Types.INT(),
                                Types.LONG(),
                                Types.STRING(),
                                Types.PRIMITIVE_ARRAY(Types.INT()))
                        .toArray(new TypeInformation[0]),
                new String[] {"a", "b", "c", "d"});
        util.addTableSource(
                "MyTable2",
                ImmutableList.<TypeInformation<?>>of(
                                Types.INT(),
                                Types.LONG(),
                                Types.STRING(),
                                Types.PRIMITIVE_ARRAY(Types.INT()))
                        .toArray(new TypeInformation[0]),
                new String[] {"a2", "b2", "c2", "d2"});
        util.addTemporarySystemFunction("func1", new AsyncCalcSplitRuleTest.Func1());
        util.addTemporarySystemFunction("func2", new AsyncCalcSplitRuleTest.Func2());
        util.addTemporarySystemFunction("func3", new AsyncCalcSplitRuleTest.Func3());
        util.addTemporarySystemFunction("func4", new AsyncCalcSplitRuleTest.Func4());
        util.addTemporarySystemFunction("func5", new AsyncCalcSplitRuleTest.Func5());
    }

    @Test
    public void testInnerJoinWithFuncInOn() {
        String sqlQuery =
                "SELECT a from MyTable INNER JOIN MyTable2 ON func2(a) = func2(a2) AND "
                        + "REGEXP(func2(a), 'string (2|4)')";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testInnerJoinWithFuncProjection() {
        String sqlQuery = "SELECT func1(a) from MyTable INNER JOIN MyTable2 ON a = a2";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testInnerJoinWithFuncInWhere() {
        String sqlQuery =
                "SELECT a from MyTable INNER JOIN MyTable2 ON a = a2 "
                        + "WHERE REGEXP(func2(a), 'val (2|3)')";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testLeftJoinWithFuncInOn() {
        String sqlQuery = "SELECT a, a2 from MyTable LEFT JOIN MyTable2 ON func1(a) = func1(a2)";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testLeftJoinWithFuncInWhere() {
        String sqlQuery =
                "SELECT a, a2 from MyTable LEFT JOIN MyTable2 ON a = a2 "
                        + "WHERE REGEXP(func2(a), 'string (2|3)')";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testRightJoinWithFuncInOn() {
        String sqlQuery = "SELECT a, a2 from MyTable RIGHT JOIN MyTable2 ON func1(a) = func1(a2)";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testProjectCallInSubquery() {
        String sqlQuery =
                "SELECT blah FROM (SELECT func2(a) as blah from MyTable) "
                        + "WHERE REGEXP(blah, 'string (2|3)')";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testWhereConditionCallInSubquery() {
        String sqlQuery =
                "SELECT blah FROM (select a as blah from MyTable "
                        + "WHERE REGEXP(func2(a), 'string (2|3)'))";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testWhereNotInSubquery() {
        String sqlQuery = "SELECT func1(a) FROM MyTable where a not in (select a2 from MyTable2)";
        util.verifyRelPlan(sqlQuery);
    }
}
