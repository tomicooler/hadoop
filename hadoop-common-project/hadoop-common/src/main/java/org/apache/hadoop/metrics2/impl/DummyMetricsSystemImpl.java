package org.apache.hadoop.metrics2.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsAnnotations;

@InterfaceAudience.Private
@Metrics(context="dummymetricssystem")
public class DummyMetricsSystemImpl extends MetricsSystem {
  @Override
  public MetricsSystem init(String prefix) {
    return this;
  }

  @Override
  public <T> T register(String name, String desc, T source) {
    MetricsAnnotations.newSourceBuilder(source).build();
    return source;
  }

  @Override
  public void unregisterSource(String name) {
  }

  @Override
  public MetricsSource getSource(String name) {
    return null;
  }

  @Override
  public <T extends MetricsSink> T register(String name, String desc, T sink) {
    return null;
  }

  @Override
  public void register(Callback callback) {
  }

  @Override
  public void publishMetricsNow() {
  }

  @Override
  public boolean shutdown() {
    return true;
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  @Override
  public void startMetricsMBeans() {
  }

  @Override
  public void stopMetricsMBeans() {
  }

  @Override
  public String currentConfig() {
    return null;
  }
}
