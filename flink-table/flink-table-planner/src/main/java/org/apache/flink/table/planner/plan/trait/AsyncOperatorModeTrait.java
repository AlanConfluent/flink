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
