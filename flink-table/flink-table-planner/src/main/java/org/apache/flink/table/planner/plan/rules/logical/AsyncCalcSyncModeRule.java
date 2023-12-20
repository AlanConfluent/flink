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

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRel;
import org.apache.flink.table.planner.plan.trait.AsyncOperatorModeTrait;
import org.apache.flink.table.planner.plan.utils.AsyncUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Rules for putting an async operator into sync mode. This should only happen if we run into query
 * structures we do not support.
 */
public class AsyncCalcSyncModeRule {
    public static final RelOptRule SYNC_MODE_CALC_JOIN = new AsyncCalcSyncModeCalcJoinRule();
    public static final RelOptRule SYNC_MODE_JOIN_CALC = new AsyncCalcSyncModeJoinCalcRule();

    public static class AsyncCalcSyncModeCalcJoinRule extends RelOptRule {

        protected AsyncCalcSyncModeCalcJoinRule() {
            super(
                    operand(FlinkLogicalCalc.class, operand(FlinkLogicalJoin.class, any())),
                    "AsyncCalcSyncModeCalcJoinRule");
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            FlinkLogicalCalc calc = call.rel(0);
            return containsAsync(calc);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            FlinkLogicalCalc calc = call.rel(0);
            FlinkLogicalRel rel = call.rel(1);
            Calc newCalc =
                    calc.copy(
                            calc.getTraitSet().replace(AsyncOperatorModeTrait.SYNC_MODE),
                            calc.getInputs());
            call.transformTo(newCalc);
        }
    }

    public static class AsyncCalcSyncModeJoinCalcRule extends RelOptRule {
        protected AsyncCalcSyncModeJoinCalcRule() {
            super(
                    operand(
                            FlinkLogicalJoin.class,
                            some(
                                    operand(FlinkLogicalCalc.class, any()),
                                    operand(FlinkLogicalCalc.class, any()))),
                    "AsyncCalcSyncModeJoinCalcRule");
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            FlinkLogicalJoin join = call.rel(0);
            FlinkLogicalCalc calc1 = call.rel(1);
            boolean containsAsync = false;
            if (call.getRelList().size() >= 3) {
                containsAsync = containsAsync(call.rel(2));
            }
            return containsAsync(calc1) || containsAsync;
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            FlinkLogicalJoin join = call.rel(0);
            FlinkLogicalCalc calc = call.rel(1);
            FlinkLogicalCalc calc2 = call.getRelList().size() >= 3 ? call.rel(2) : null;
            RelNode left = trimHep(join.getLeft());
            if (left.equals(calc) || left.equals(calc2)) {
                left =
                        left.copy(
                                left.getTraitSet().replace(AsyncOperatorModeTrait.SYNC_MODE),
                                left.getInputs());
            }
            RelNode right = trimHep(join.getRight());
            if (right.equals(calc) || right.equals(calc2)) {
                right =
                        right.copy(
                                right.getTraitSet().replace(AsyncOperatorModeTrait.SYNC_MODE),
                                right.getInputs());
            }

            Join newJoin =
                    join.copy(
                            join.getTraitSet(),
                            join.getCondition(),
                            left,
                            right,
                            join.getJoinType(),
                            join.isSemiJoinDone());

            call.transformTo(newJoin);
        }
    }

    private static boolean containsAsync(FlinkLogicalCalc calc) {
        List<RexNode> projects =
                calc.getProgram().getProjectList().stream()
                        .map(calc.getProgram()::expandLocalRef)
                        .collect(Collectors.toList());
        return Optional.ofNullable(calc.getProgram().getCondition())
                        .map(calc.getProgram()::expandLocalRef)
                        .filter(AsyncUtil::containsAsyncCall)
                        .isPresent()
                || projects.stream().anyMatch(AsyncUtil::containsAsyncCall);
    }

    private static RelNode trimHep(RelNode node) {
        if (node instanceof HepRelVertex) {
            return ((HepRelVertex) node).getCurrentRel();
        }
        return node;
    }
}
