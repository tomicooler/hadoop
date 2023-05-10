/**
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

package org.apache.hadoop.metrics2.lib;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A mutable int gauge
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableGaugeIntTomi extends MutableGauge {

  public Object parent = null;

  private static final Logger LOG =
      LoggerFactory.getLogger(MutableGaugeIntTomi.class);

  private AtomicInteger value = new AtomicInteger();

  MutableGaugeIntTomi(MetricsInfo info, int initValue) {
    super(info);
    LOG.error("tomi MutableGaugeIntTomi {} {}", parent, initValue);
    this.value.set(initValue);
  }

  public int value() {
    LOG.error("tomi MutableGaugeIntTomi return value parent {} : '{}'", parent, value.get());
    Arrays.stream(Thread.currentThread().getStackTrace()).forEach(s -> System.out.println(
        "\tat " + s.getClassName() + "." + s.getMethodName() + "(" + s.getFileName() + ":" + s
            .getLineNumber() + ")"));
    return value.get();
  }

  @Override
  public void incr() {
    incr(1);
  }

  /**
   * Increment by delta
   * @param delta of the increment
   */
  public void incr(int delta) {
    LOG.error("tomi MutableGaugeIntTomi incr {} {}", parent, delta);
    value.addAndGet(delta);
    setChanged();
  }

  @Override
  public void decr() {
    decr(1);
  }

  /**
   * decrement by delta
   * @param delta of the decrement
   */
  public void decr(int delta) {
    LOG.error("tomi MutableGaugeIntTomi decr {} {}", parent, delta);
    value.addAndGet(-delta);
    setChanged();
  }

  /**
   * Set the value of the metric
   * @param value to set
   */
  public void set(int value) {
    LOG.error("tomi MutableGaugeIntTomi set {} {}", parent, value);
    this.value.set(value);
    setChanged();
  }

  @Override
  public void snapshot(MetricsRecordBuilder builder, boolean all) {
    LOG.error("tomi MutableGaugeIntTomi snapshot {} {} {}", parent, all, changed());
    if (all || changed()) {
      builder.addGauge(info(), value());
      clearChanged();
    }
  }

  /**
   * @return  the value of the metric
   */
  public String toString() {
    LOG.error("tomi MutableGaugeIntTomi toString {} {}", parent, value.toString());
    return value.toString();
  }
}
