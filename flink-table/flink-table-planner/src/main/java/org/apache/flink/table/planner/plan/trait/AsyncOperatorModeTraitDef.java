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
