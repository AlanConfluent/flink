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
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.plan.rules.FlinkStreamRuleSets;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.types.Row;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

/** Test for {@link AsyncCalcSplitRule}. */
public class AsyncCalcSplitRuleTest extends TableTestBase {

    private TableTestUtil util = streamTestUtil(TableConfig.getDefault());

    @Before
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
        util.addTemporarySystemFunction("func1", new Func1());
        util.addTemporarySystemFunction("func2", new Func2());
        util.addTemporarySystemFunction("func3", new Func3());
        util.addTemporarySystemFunction("func4", new Func4());
        util.addTemporarySystemFunction("func5", new Func5());
    }

    @Test
    public void testSingleCall() {
        String sqlQuery = "SELECT func1(a) FROM MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testLiteralPlusTableSelect() {
        String sqlQuery = "SELECT 'foo', func1(a) FROM MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testFieldPlusTableSelect() {
        String sqlQuery = "SELECT a, func1(a) from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testTwoCalls() {
        String sqlQuery = "SELECT func1(a), func1(a) from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testNestedCalls() {
        String sqlQuery = "SELECT func1(func1(func1(a))) from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testThreeNestedCalls() {
        String sqlQuery = "SELECT func1(func1(a)), func1(func1(func1(a))), func1(a) from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testPassedToOtherUDF() {
        String sqlQuery = "SELECT Concat(func2(a), 'foo') from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testJustCall() {
        String sqlQuery = "SELECT func1(1)";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testWhereCondition() {
        String sqlQuery = "SELECT a from MyTable where REGEXP(func2(a), 'string (2|3)')";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testWhereConditionAndProjection() {
        String sqlQuery = "SELECT func2(a) from MyTable where REGEXP(func2(a), 'val (2|3)')";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testWhereConditionWithInts() {
        String sqlQuery = "SELECT a from MyTable where func1(a) >= 12";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testAggregate() {
        String sqlQuery = "SELECT a, func3(count(*)) from MyTable group by a";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testSelectCallWithIntArray() {
        String sqlQuery = "SELECT func4(d) from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testFieldAccessAfter() {
        String sqlQuery = "SELECT func5(a).f0 from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testFieldOperand() {
        String sqlQuery = "SELECT func1(func5(a).f0) from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    public static class Func1 extends AsyncScalarFunction {
        public void eval(CompletableFuture<Integer> future, Integer param) {
            future.complete(param + 10);
        }
    }

    public static class Func2 extends AsyncScalarFunction {
        public void eval(CompletableFuture<String> future, Integer param) {
            future.complete("string " + param + 10);
        }
    }

    public static class Func3 extends AsyncScalarFunction {
        public void eval(CompletableFuture<Long> future, Long param) {
            future.complete(param + 10);
        }
    }

    public static class Func4 extends AsyncScalarFunction {
        public void eval(CompletableFuture<int[]> future, int[] param) {
            future.complete(param);
        }
    }

    public static class Func5 extends AsyncScalarFunction {

        @DataTypeHint("ROW<f0 INT, f1 String>")
        public void eval(CompletableFuture<Row> future, Integer a) {
            future.complete(Row.of(a + 1, "" + (a * a)));
        }
    }
}
