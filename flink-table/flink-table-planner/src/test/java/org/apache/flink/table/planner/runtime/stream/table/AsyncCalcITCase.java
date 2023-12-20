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

package org.apache.flink.table.planner.runtime.stream.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case tests for {@link AsyncScalarFunction}. */
public class AsyncCalcITCase extends AbstractTestBase {

    private StreamExecutionEnvironment env;

    private TableEnvironment tEnv;

    @BeforeEach
    public void before() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setMaxParallelism(1);
        tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_BUFFER_CAPACITY, 1);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_TIMEOUT, Duration.ofMinutes(1));
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_OUTPUT_MODE,
                        ExecutionConfigOptions.AsyncOutputMode.ORDERED);
    }

    @Test
    public void testSimpleTableSelect() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results = executeSql("select func(f1) from t1");
        final List<Row> expectedRows =
                ImmutableList.of(Row.of("val 1"), Row.of("val 2"), Row.of("val 3"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testLiteralPlusTableSelect() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results = executeSql("select 'foo', func(f1) from t1");
        final List<Row> expectedRows =
                ImmutableList.of(
                        Row.of("foo", "val 1"), Row.of("foo", "val 2"), Row.of("foo", "val 3"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testFieldPlusTableSelect() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results = executeSql("select f1, func(f1) from t1");
        final List<Row> expectedRows =
                ImmutableList.of(Row.of(1, "val 1"), Row.of(2, "val 2"), Row.of(3, "val 3"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testTwoCalls() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results = executeSql("select func(f1), func(f1) from t1");
        final List<Row> expectedRows =
                ImmutableList.of(
                        Row.of("val 1", "val 1"),
                        Row.of("val 2", "val 2"),
                        Row.of("val 3", "val 3"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testNestedCalls() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFuncAdd10());
        final List<Row> results = executeSql("select func(func(func(f1))) from t1");
        final List<Row> expectedRows = ImmutableList.of(Row.of(31), Row.of(32), Row.of(33));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testThreeNestedCalls() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFuncAdd10());
        final List<Row> results =
                executeSql("select func(func(f1)), func(func(func(f1))), func(f1) from t1");
        final List<Row> expectedRows =
                ImmutableList.of(Row.of(21, 31, 11), Row.of(22, 32, 12), Row.of(23, 33, 13));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testPassedToOtherUDF() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results = executeSql("select Concat(func(f1), 'foo') from t1");
        final List<Row> expectedRows =
                ImmutableList.of(Row.of("val 1foo"), Row.of("val 2foo"), Row.of("val 3foo"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testJustCall() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results = executeSql("select func(1)");
        final List<Row> expectedRows = ImmutableList.of(Row.of("val 1"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testWhereCondition() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results =
                executeSql("select f1 from t1 where REGEXP(func(f1), 'val (2|3)')");
        final List<Row> expectedRows = ImmutableList.of(Row.of(2), Row.of(3));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testWhereConditionAndProjection() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results =
                executeSql("select func(f1) from t1 where REGEXP(func(f1), 'val (2|3)')");
        final List<Row> expectedRows = ImmutableList.of(Row.of("val 2"), Row.of("val 3"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testWhereConditionWithInts() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFuncAdd10());
        final List<Row> results = executeSql("select f1 from t1 where func(f1) >= 12");
        final List<Row> expectedRows = ImmutableList.of(Row.of(2), Row.of(3));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testAggregate() {
        Table t1 = tEnv.fromValues(1, 2, 3, 1, 3, 4).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFuncAdd10Long());
        final List<Row> results = executeSql("select f1, func(count(*)) from t1 group by f1");
        final List<Row> expectedRows =
                ImmutableList.of(
                        Row.of(1, 11L),
                        Row.of(2, 11L),
                        Row.of(3, 11L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 11L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 12L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 3, 11L),
                        Row.ofKind(RowKind.UPDATE_AFTER, 3, 12L),
                        Row.of(4, 11L));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testSelectCallWithIntArray() {
        Table t1 = tEnv.fromValues(new int[] {1, 2}, new int[] {3, 4}).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFuncAdd10IntArray());
        final List<Row> results = executeSql("select func(f1) from t1");
        final List<Row> expectedRows =
                ImmutableList.of(
                        Row.of(new Object[] {new Integer[] {11, 12}}),
                        Row.of(new Object[] {new Integer[] {13, 14}}));
        // When run here, the plan is a union of the two AsyncCalcs so order is
        // not retained!
        assertThat(results).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    @Test
    public void testInnerJoinWithFuncInOn() {
        Table t1 = tEnv.fromValues(1, 2, 3, 4).as("f1");
        Table t2 = tEnv.fromValues(2, 4).as("f2");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryView("t2", t2);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results =
                executeSql(
                        true,
                        "select f1 from t1 INNER JOIN t2 ON func(f1) = func(f2) AND REGEXP(func(f1), 'val (2|4)')");
        final List<Row> expectedRows = ImmutableList.of(Row.of(2), Row.of(4));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testInnerJoinWithFuncProjection() {
        Table t1 = tEnv.fromValues(1, 2, 3, 4).as("f1");
        Table t2 = tEnv.fromValues(2, 4).as("f2");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryView("t2", t2);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results =
                executeSql(true, "select func(f1) from t1 INNER JOIN t2 ON f1 = f2");
        final List<Row> expectedRows = ImmutableList.of(Row.of("val 2"), Row.of("val 4"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testInnerJoinWithFuncInWhere() {
        Table t1 = tEnv.fromValues(1, 2, 3, 4).as("f1");
        Table t2 = tEnv.fromValues(2, 4).as("f2");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryView("t2", t2);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results =
                executeSql(
                        true,
                        "select f1 from t1 INNER JOIN t2 ON f1 = f2 WHERE REGEXP(func(f1), 'val (2|3)')");
        final List<Row> expectedRows = ImmutableList.of(Row.of(2));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testLeftJoinWithFuncInOn() {
        Table t1 = tEnv.fromValues(1, 2, 3, 4).as("f1");
        Table t2 = tEnv.fromValues(2, 4).as("f2");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryView("t2", t2);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results =
                executeSql(true, "select f1, f2 from t1 LEFT JOIN t2 ON func(f1) = func(f2)");
        final List<Row> expectedRows =
                ImmutableList.of(Row.of(1, null), Row.of(2, 2), Row.of(3, null), Row.of(4, 4));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testLeftJoinWithFuncInWhere() {
        Table t1 = tEnv.fromValues(1, 2, 3, 4).as("f1");
        Table t2 = tEnv.fromValues(2, 4).as("f2");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryView("t2", t2);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results =
                executeSql(
                        true,
                        "select f1, f2 from t1 LEFT JOIN t2 ON f1 = f2 WHERE REGEXP(func(f1), 'val (2|3)')");
        final List<Row> expectedRows = ImmutableList.of(Row.of(2, 2), Row.of(3, null));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testRightJoinWithFuncInOn() {
        Table t1 = tEnv.fromValues(1, 2, 3, 4).as("f1");
        Table t2 = tEnv.fromValues(2, 4).as("f2");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryView("t2", t2);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results =
                executeSql(true, "select f1, f2 from t1 FULL OUTER JOIN t2 ON func(f1) = func(f2)");
        assertThat(results).hasSize(8);
    }

    @Test
    public void testSelectWithConfigs() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_BUFFER_CAPACITY.key(), "10");
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_TIMEOUT.key(), "1m");
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_OUTPUT_MODE.key(), "ORDERED");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results = executeSql("select func(f1) from t1");
        final List<Row> expectedRows =
                ImmutableList.of(Row.of("val 1"), Row.of("val 2"), Row.of("val 3"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testProjectCallInSubquery() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results =
                executeSql(
                        "select blah FROM (select func(f1) as blah from t1) "
                                + "WHERE REGEXP(blah, 'val (2|3)')");
        final List<Row> expectedRows = ImmutableList.of(Row.of("val 2"), Row.of("val 3"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testWhereConditionCallInSubquery() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results =
                executeSql(
                        "select blah FROM (select f1 as blah from t1 "
                                + "WHERE REGEXP(func(f1), 'val (2|3)'))");
        final List<Row> expectedRows = ImmutableList.of(Row.of(2), Row.of(3));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testWhereNotInSubquery() {
        Table t1 = tEnv.fromValues(1, 2, 3, 4).as("f1");
        Table t2 = tEnv.fromValues(2, 4).as("f2");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryView("t2", t2);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results =
                executeSql(true, "select func(f1) FROM t1 where f1 not in (select f2 from t2)");
        final List<Row> expectedRows = ImmutableList.of(Row.of("val 1"), Row.of("val 3"));
        assertThat(results).containsSubsequence(expectedRows);
    }

    @Test
    public void testSimpleTableSelectWithFallback() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        Table t2 = tEnv.fromValues(2, 4).as("f2");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryView("t2", t2);
        tEnv.createTemporaryFunction("func", new AsyncFunc());
        final List<Row> results =
                executeSql(true, "select func(f1) from t1 INNER JOIN t2 ON f1 = f2");
        final List<Row> expectedRows = ImmutableList.of(Row.of("val 2"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testFieldAccessAfter() {
        Table t1 = tEnv.fromValues(2).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFuncRow());
        final List<Row> results = executeSql("select func(f1).f0 from t1");
        final List<Row> expectedRows = ImmutableList.of(Row.of(3));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testFieldOperand() {
        Table t1 = tEnv.fromValues(2).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFuncRow());
        tEnv.createTemporaryFunction("func2", new AsyncFuncAdd10());
        Table structs = tEnv.sqlQuery("select func(f1) from t1");
        tEnv.createTemporaryView("t2", structs);
        final List<Row> results = executeSql("select func2(t2.f0) from t2");
        final List<Row> expectedRows = ImmutableList.of(Row.of(13));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testOverload() {
        Table t1 = tEnv.fromValues(1).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new AsyncFuncOverload());
        final List<Row> results = executeSql("select func(f1), func(cast(f1 as String)) from t1");
        final List<Row> expectedRows =
                ImmutableList.of(Row.of("int version 1", "string version 1"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testMultiLayerGeneric() {
        Table t1 = tEnv.fromValues(1).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporaryFunction("func", new LongAsyncFuncGeneric());
        final List<Row> results = executeSql("select func(f1) from t1");
        final List<Row> expectedRows = ImmutableList.of(Row.of(11L));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testFailures() {
        Table t1 = tEnv.fromValues(1).as("f1");
        tEnv.createTemporaryView("t1", t1);
        AsyncFuncFail func = new AsyncFuncFail(2);
        tEnv.createTemporaryFunction("func", func);
        final List<Row> results = executeSql("select func(f1) from t1");
        final List<Row> expectedRows = ImmutableList.of(Row.of(3));
        assertThat(results).containsSequence(expectedRows);
    }

    private List<Row> executeSql(String sql) {
        return executeSql(false, sql);
    }

    private List<Row> executeSql(boolean expectSyncMode, String sql) {
        String explain = tEnv.explainSql(sql);
        assertThat(explain)
                .containsPattern(
                        String.format("AsyncCalc\\(.*syncMode=\\[%b].*\\)", expectSyncMode));
        TableResult result = tEnv.executeSql(sql);
        final List<Row> rows = new ArrayList<>();
        result.collect().forEachRemaining(rows::add);
        return rows;
    }

    public static class AsyncFunc extends AsyncFuncBase {

        private static final long serialVersionUID = 1L;

        public void eval(CompletableFuture<String> future, Integer param) {
            executor.schedule(() -> future.complete("val " + param), 10, TimeUnit.MILLISECONDS);
        }
    }

    public static class AsyncFuncAdd10 extends AsyncFuncBase {

        private static final long serialVersionUID = 2L;

        public void eval(CompletableFuture<Integer> future, Integer param) {
            executor.schedule(() -> future.complete(param + 10), 10, TimeUnit.MILLISECONDS);
        }
    }

    public static class AsyncFuncOverload extends AsyncFuncBase {

        private static final long serialVersionUID = 3L;

        public void eval(CompletableFuture<String> future, Integer param) {
            executor.schedule(
                    () -> future.complete("int version " + param), 10, TimeUnit.MILLISECONDS);
        }

        public void eval(CompletableFuture<String> future, String param) {
            executor.schedule(
                    () -> future.complete("string version " + param), 10, TimeUnit.MILLISECONDS);
        }
    }

    public static class AsyncFuncAdd10Long extends AsyncFuncBase {

        private static final long serialVersionUID = 2L;

        public void eval(CompletableFuture<Long> future, Long param) {
            executor.schedule(() -> future.complete(param + 10), 10, TimeUnit.MILLISECONDS);
        }
    }

    public static class AsyncFuncAdd10IntArray extends AsyncFuncBase {

        private static final long serialVersionUID = 3L;

        public void eval(CompletableFuture<int[]> future, int[] param) {
            for (int i = 0; i < param.length; i++) {
                param[i] += 10;
            }
            executor.schedule(() -> future.complete(param), 10, TimeUnit.MILLISECONDS);
        }
    }

    public static class AsyncFuncRow extends AsyncScalarFunction {

        @DataTypeHint("ROW<f0 INT, f1 String>")
        public void eval(CompletableFuture<Row> future, int a) {
            future.complete(Row.of(a + 1, "" + (a * a)));
        }
    }

    public static class AsyncFuncFail extends AsyncFuncBase implements Serializable {

        private static final long serialVersionUID = 8996145425452974113L;

        private final int numFailures;
        private final AtomicInteger failures = new AtomicInteger(0);

        public AsyncFuncFail(int numFailures) {
            this.numFailures = numFailures;
        }

        public void eval(CompletableFuture<Integer> future, int a) {
            if (failures.getAndIncrement() < numFailures) {
                future.completeExceptionally(new RuntimeException("Error " + failures.get()));
                return;
            }
            future.complete(failures.get());
        }
    }

    public abstract static class AsyncFuncGeneric<T> extends AsyncFuncBase {

        private static final long serialVersionUID = 3L;

        abstract T newT(int param);

        public void eval(CompletableFuture<T> future, Integer param) {
            executor.schedule(() -> future.complete(newT(param)), 10, TimeUnit.MILLISECONDS);
        }
    }

    public static class LongAsyncFuncGeneric extends AsyncFuncGeneric<Long> {
        @Override
        Long newT(int param) {
            return 10L + param;
        }
    }

    public static class AsyncFuncBase extends AsyncScalarFunction {

        protected ScheduledExecutorService executor;

        @Override
        public void open(FunctionContext context) {
            executor = Executors.newSingleThreadScheduledExecutor();
        }

        @Override
        public void close() {
            if (null != executor && !executor.isShutdown()) {
                executor.shutdownNow();
            }
        }
    }
}
