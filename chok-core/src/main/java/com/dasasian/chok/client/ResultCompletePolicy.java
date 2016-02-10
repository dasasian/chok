/**
 * Copyright (C) 2014 Dasasian (damith@dasasian.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dasasian.chok.client;

/**
 * Wait for the results to be fully and/or partially complete (based on number
 * of shards), the result is closed, or N msec has passed, whichever comes
 * first. The resulting ClientResult may be closed or left open, depending on
 * the shutDown setting passed to the constructor. This class does not look at
 * the results themselves (type T), it only considers the number of shards
 * reporting (with a T result, or a Throwable if an error occured) compared to
 * the total number of shards.
 * <p>
 * If you must return in 5 seconds use new ResultCompletePolicy(5000). If you
 * want to do your own polling, use new ResultCompletePolicy(0, false). If you
 * want to wait a minimum of 3 seconds (but return sooner if results are
 * complete), then wait another 2 seconds for 95% coverage (shard based), then
 * use new ResultCompletePolicy(3000, 2000, 0.95, true).
 * <p>
 * You could also write a custom IResultPolicy that looks inside the result
 * objects to decide how much longer to wait.
 */
public class ResultCompletePolicy<T> implements IResultPolicy<T> {

    private final long completeWait;
    private final long completeStopTime;
    private final long coverageWait;
    private final long coverageStopTime;
    private final double coverage;

    /**
     * Returns a policy that waits for the results to be complete (all shards reporting with results or
     * errors), result is closed, or N msec has passed, whichever comes first.
     *
     * @param timeout Max msec to wait for results.
     * @param <T> the type of the response
     * @return the result completion policy
     *
     */
    public static <T> ResultCompletePolicy<T> awaitCompletion(long timeout) {
        return new ResultCompletePolicy<>(timeout, 0, 1.0);
    }

//    /**
//     * Returns a policy that waits for the results to be complete (all shards reporting with results or
//     * errors), result is closed, or N msec has passed, whichever comes first. Then close the result,
//     * shutting down the call.
//     *
//     * @param timeout Max msec to wait for results.
//     */
//    @Deprecated
//    public static <T> ResultCompletePolicy<T> awaitCompletionThenShutdown(long timeout) {
//        return new ResultCompletePolicy<>(timeout, 0, 1.0);
//    }

    /**
     * Returns a policy that for the results to complete (all shards reporting a result or error),
     * the results to be closed, or completeWait msec, whichever comes first. Then
     * if not complete and not closed, wait for the results to be closed, shard
     * coverage to be &gt;= coverage, or coverageWait msec, whichever comes first. If
     * shutDown is set, close the result which terminates the call.
     *
     * @param completeWait How long (msec) to wait for complete results.
     * @param coverageWait How long (msec, after completeWait) to wait for coverage to meet
     *                     or exceed coverage param.
     * @param coverage     The required coverage (0.0 .. 1.0) when waiting for coverage. Not
     *                     used if coverageWait = 0.
     * @param <T> the type of the response
     * @return the result completion policy
     */
    public static <T> ResultCompletePolicy<T> awaitCompletion(long completeWait, long coverageWait, double coverage) {
        return new ResultCompletePolicy<>(completeWait, coverageWait, coverage);
    }

    /**
     * Wait for the results to complete (all shards reporting a result or error),
     * the results to be closed, or completeWait msec, whichever comes first. Then
     * if not complete and not closed, wait for the results to be closed, shard
     * coverage to be &gt;= coverage, or coverageWait msec, whichever comes first. If
     * shutDown is set, close the result which terminates the call.
     *
     * @param completeWait How long (msec) to wait for complete results.
     * @param coverageWait How long (msec, after completeWait) to wait for coverage to meet
     *                     or exceed coverage param.
     * @param coverage     The required coverage (0.0 .. 1.0) when waiting for coverage. Not
     *                     used if coverageWait = 0.
     */
    private ResultCompletePolicy(long completeWait, long coverageWait, double coverage) {
        long now = System.currentTimeMillis();
        if (completeWait < 0 || coverageWait < 0) {
            throw new IllegalArgumentException("Wait times must be >= 0");
        }
        if (coverage < 0.0 || coverage > 1.0) {
            throw new IllegalArgumentException("Coverage must be 0.0 .. 1.0");
        }
        this.completeWait = completeWait;
        this.coverageWait = coverageWait;
        completeStopTime = now + completeWait;
        coverageStopTime = now + completeWait + coverageWait;
        this.coverage = coverage;
    }

    /**
     * How much longer, if any, should we wait for results to arrive. Also, should
     * WorkQueue be shut down, and the ClientResult closed?
     *
     * @param result The results we have so far.
     * @return if &gt; 0, sleep at most that many msec, or until a new result
     * arrives, or the result is closed, whichever comes first. Then call
     * this method again. If 0, stop waiting and return the result
     * immediately. if &lt; 0, shutdown the WorkQueue, close the result, and
     * return it immediately.
     */
    @Override
    public long waitTime(IResultReceiverWrapper<T> result) {
        boolean done = result.isClosed();
        long now = System.currentTimeMillis();
        if (!done) {
            if (now < completeStopTime) {
                done = result.isComplete();
            } else if (now < coverageStopTime) {
                done = result.getShardCoverage() >= coverage;
            } else {
                done = true;
            }
        }
        if (done) {
            return 0;
        }
        return coverageStopTime - now;
    }

    @Override
    public String toString() {
        String s = "Wait up to " + completeWait + " ms for complete results";
        if (coverageWait > 0) {
            s += ", then " + coverageWait + " ms for " + coverage + " coverage";
        }
        s += ".";
        return s;
    }

}
