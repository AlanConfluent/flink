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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.AsyncCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AsyncUtil;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.calc.async.AsyncFunctionRunner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Base class for exec Async Calc. */
public abstract class CommonExecAsyncCalc extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(CommonExecAsyncCalc.class);

    public static final String ASYNC_CALC_TRANSFORMATION = "async-calc";

    public static final String FIELD_NAME_PROJECTION = "projection";

    public static final String FIELD_NAME_SYNC_MODE = "syncMode";

    @JsonProperty(FIELD_NAME_PROJECTION)
    private final List<RexNode> projection;

    @JsonProperty(FIELD_NAME_SYNC_MODE)
    private final boolean syncMode;

    public CommonExecAsyncCalc(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            List<RexNode> projection,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description,
            boolean syncMode) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.projection = checkNotNull(projection);
        this.syncMode = syncMode;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        return createAsyncOneInputTransformation(
                inputTransform, config, planner.getFlinkContext().getClassLoader());
    }

    private OneInputTransformation<RowData, RowData> createAsyncOneInputTransformation(
            Transformation<RowData> inputTransform,
            ExecNodeConfig config,
            ClassLoader classLoader) {
        List<RexCall> asyncRexCalls =
                projection.stream()
                        .filter(x -> x instanceof RexCall)
                        .map(x -> (RexCall) x)
                        .collect(Collectors.toList());

        List<Integer> forwardedFields =
                projection.stream()
                        .filter(x -> x instanceof RexInputRef)
                        .map(x -> ((RexInputRef) x).getIndex())
                        .collect(Collectors.toList());
        InternalTypeInfo<RowData> asyncOperatorInputTypeInfo =
                (InternalTypeInfo<RowData>) inputTransform.getOutputType();
        LogicalType[] inputLogicalTypes =
                ((InternalTypeInfo<RowData>) inputTransform.getOutputType()).toRowFieldTypes();

        List<LogicalType> forwardedFieldsLogicalTypes =
                forwardedFields.stream()
                        .map(i -> inputLogicalTypes[i])
                        .collect(Collectors.toList());
        List<LogicalType> asyncCallLogicalTypes =
                asyncRexCalls.stream()
                        .map(node -> FlinkTypeFactory.toLogicalType(node.getType()))
                        .collect(Collectors.toList());
        List<LogicalType> fieldsLogicalTypes = new ArrayList<>();
        fieldsLogicalTypes.addAll(forwardedFieldsLogicalTypes);
        fieldsLogicalTypes.addAll(asyncCallLogicalTypes);
        InternalTypeInfo<RowData> asyncOperatorResultTypeInfo =
                InternalTypeInfo.ofFields(fieldsLogicalTypes.toArray(new LogicalType[0]));
        OneInputStreamOperatorFactory<RowData, RowData> factory =
                getAsyncFunctionOperator(config, classLoader, asyncOperatorInputTypeInfo);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(ASYNC_CALC_TRANSFORMATION, config),
                factory,
                asyncOperatorResultTypeInfo,
                inputTransform.getParallelism(),
                false);
    }

    private OneInputStreamOperatorFactory<RowData, RowData> getAsyncFunctionOperator(
            ExecNodeConfig config,
            ClassLoader classLoader,
            InternalTypeInfo<RowData> inputRowTypeInfo) {

        List<LogicalType> resultTypes =
                projection.stream()
                        .map(node -> FlinkTypeFactory.toLogicalType(node.getType()))
                        .collect(Collectors.toList());
        InternalTypeInfo<RowData> resultTyeInfo =
                InternalTypeInfo.ofFields(resultTypes.toArray(new LogicalType[0]));

        GeneratedFunction<AsyncFunction<RowData, RowData>> generatedFunction =
                AsyncCodeGenerator.generateFunction(
                        "AsyncScalarFunction",
                        inputRowTypeInfo.toRowType(),
                        resultTyeInfo.toRowType(),
                        GenericRowData.class,
                        projection,
                        true,
                        config,
                        classLoader);
        AsyncFunctionRunner func = new AsyncFunctionRunner(generatedFunction, syncMode);
        AsyncUtil.Options options = AsyncUtil.getAsyncOptions(config);
        return new AsyncWaitOperatorFactory<>(
                func,
                options.asyncTimeout,
                options.asyncBufferCapacity,
                options.asyncOutputMode,
                options.asyncRetryStrategy);
    }
}
