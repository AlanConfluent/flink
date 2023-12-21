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
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

public class AsyncOperatorModeTrait implements RelTrait {

    public static final AsyncOperatorModeTrait SYNC_MODE = new AsyncOperatorModeTrait(true);

    public static final AsyncOperatorModeTrait ASYNC_MODE = new AsyncOperatorModeTrait(false);

    private final boolean syncMode;

    public AsyncOperatorModeTrait(boolean syncMode) {
        this.syncMode = syncMode;
    }

    @Override
    public RelTraitDef<? extends RelTrait> getTraitDef() {
        return AsyncOperatorModeTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait relTrait) {
        if (relTrait instanceof AsyncOperatorModeTrait) {
            return ((AsyncOperatorModeTrait) relTrait).syncMode == this.syncMode;
        }
        return false;
    }

    @Override
    public void register(RelOptPlanner relOptPlanner) {}

    @Override
    public String toString() {
        if (syncMode) {
            return "sync_mode";
        } else {
            return "async_mode";
        }
    }

    public boolean getSyncMode() {
        return syncMode;
    }
}
