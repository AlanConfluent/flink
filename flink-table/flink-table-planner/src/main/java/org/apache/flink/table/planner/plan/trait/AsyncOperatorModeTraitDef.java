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

package org.apache.flink.table.planner.plan.trait;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class AsyncOperatorModeTraitDef extends RelTraitDef<AsyncOperatorModeTrait> {

    public static final AsyncOperatorModeTraitDef INSTANCE = new AsyncOperatorModeTraitDef();

    @Override
    public AsyncOperatorModeTrait getDefault() {
        return AsyncOperatorModeTrait.ASYNC_MODE;
    }

    @Override
    public Class<AsyncOperatorModeTrait> getTraitClass() {
        return AsyncOperatorModeTrait.class;
    }

    @Override
    public String getSimpleName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public boolean canConvert(
            RelOptPlanner planner,
            AsyncOperatorModeTrait fromTrait,
            AsyncOperatorModeTrait toTrait) {
        return true;
    }

    @Override
    public @Nullable RelNode convert(
            RelOptPlanner relOptPlanner,
            RelNode relNode,
            AsyncOperatorModeTrait asyncOperatorModeTrait,
            boolean b) {
        throw new RuntimeException("Don't invoke AsyncOperatorModeTraitDef.convert directly!");
    }
}
