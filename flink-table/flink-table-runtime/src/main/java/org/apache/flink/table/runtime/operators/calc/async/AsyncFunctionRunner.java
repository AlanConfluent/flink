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

package org.apache.flink.table.runtime.operators.calc.async;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.join.lookup.AsyncLookupJoinRunner;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/** Async function runner for AsyncScalarFunctions influenced by {@link AsyncLookupJoinRunner} */
public class AsyncFunctionRunner extends RichAsyncFunction<RowData, RowData> {

    private static final long serialVersionUID = -6664660022391632123L;

    private final GeneratedFunction<AsyncFunction<RowData, RowData>> generatedFetcher;
    private final boolean syncMode;

    private transient AsyncFunction<RowData, RowData> fetcher;

    public AsyncFunctionRunner(
            GeneratedFunction<AsyncFunction<RowData, RowData>> generatedFetcher, boolean syncMode) {
        this.generatedFetcher = generatedFetcher;
        this.syncMode = syncMode;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        fetcher = generatedFetcher.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext());
        FunctionUtils.openFunction(fetcher, openContext);
    }

    @Override
    public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) {
        try {
            if (syncMode) {
                ResultFutureWrapper wrapper = new ResultFutureWrapper(resultFuture);
                fetcher.asyncInvoke(input, wrapper);
                wrapper.waitOnCompletion();
            } else {
                fetcher.asyncInvoke(input, resultFuture);
            }
        } catch (Throwable t) {
            resultFuture.completeExceptionally(t);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        FunctionUtils.closeFunction(fetcher);
    }

    /**
     * Wraps an existing ResultFuture so that it can catch completion events and allow for waiting,
     * to implement synchronous mode.
     */
    private static class ResultFutureWrapper implements ResultFuture<RowData> {

        private final ResultFuture<RowData> resultFuture;
        private CompletableFuture<Void> done = new CompletableFuture<>();

        public ResultFutureWrapper(ResultFuture<RowData> resultFuture) {
            this.resultFuture = resultFuture;
        }

        @Override
        public void complete(Collection<RowData> result) {
            resultFuture.complete(result);
            done.complete(null);
        }

        @Override
        public void completeExceptionally(Throwable error) {
            resultFuture.completeExceptionally(error);
            done.complete(null);
        }

        public void waitOnCompletion() throws ExecutionException, InterruptedException {
            done.get();
        }
    }
}
