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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import com.google.inject.Guice;
import com.sun.jersey.api.client.ClientResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.junit.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigGeneratorForTest.createConfiguration;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestRMWebServicesCapacitySched.assertJsonResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestRMWebServicesCapacitySched.createMockRM;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestRMWebServicesCapacitySched.createWebAppDescriptor;

/**
 * The queues are configured in each test so that the effectiveMinResource is the same.
 * This makes it possible to compare the JSONs among the tests.
 *                                         EffectiveMin (32GB 32VCores)     AbsoluteCapacity
 *     root.default              4/32      [memory=4096,    vcores=4]       12.5%
 *     root.test_1              16/32      [memory=16384,   vcores=16]
 *     root.test_1.test_1_1        2/16      [memory=2048,  vcores=2]       6.25%
 *     root.test_1.test_1_2        2/16      [memory=2048,  vcores=2]       6.25%
 *     root.test_1.test_1_3       12/16      [memory=12288, vcores=12]      37.5%
 *     root.test_2              12/32      [memory=12288,   vcores=12]      37.5%
 */
public class TestRMWebServicesCapacitySchedulerMixedMode extends JerseyTestBase {

  private static final String EXPECTED_FILE_TMPL = "webapp/mixed-%s-%d.json";

  private MockRM rm;

  public TestRMWebServicesCapacitySchedulerMixedMode() {
    super(createWebAppDescriptor());
  }


  @Test
  public void testSchedulerAbsoluteAndPercentage()
      throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.legacy-queue-mode.enabled", "false");
    conf.put("yarn.scheduler.capacity.root.queues", "default, test_1, test_2");
    conf.put("yarn.scheduler.capacity.root.test_1.queues", "test_1_1, test_1_2, test_1_3");
    conf.put("yarn.scheduler.capacity.root.default.capacity", "25");
    conf.put("yarn.scheduler.capacity.root.test_1.capacity", "[memory=16384, vcores=16]");
    conf.put("yarn.scheduler.capacity.root.test_2.capacity", "75");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_1.capacity", "[memory=2048, vcores=2]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_2.capacity", "[memory=2048, vcores=2]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_3.capacity", "100");
    runTest(createConfiguration(conf));
  }

  @Test
  public void testSchedulerAbsoluteAndPercentageUsingCapacityVector()
      throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.legacy-queue-mode.enabled", "false");
    conf.put("yarn.scheduler.capacity.root.queues", "default, test_1, test_2");
    conf.put("yarn.scheduler.capacity.root.test_1.queues", "test_1_1, test_1_2, test_1_3");
    conf.put("yarn.scheduler.capacity.root.default.capacity", "[memory=25%, vcores=25%]");
    conf.put("yarn.scheduler.capacity.root.test_1.capacity", "[memory=16384, vcores=16]");
    conf.put("yarn.scheduler.capacity.root.test_2.capacity", "[memory=75%, vcores=75%]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_1.capacity", "[memory=2048, vcores=2]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_2.capacity", "[memory=2048, vcores=2]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_3.capacity", "[memory=100%, vcores=100%]");
    runTest("testSchedulerAbsoluteAndPercentage", createConfiguration(conf));
  }

  @Test
  public void testSchedulerAbsoluteAndWeight()
      throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.legacy-queue-mode.enabled", "false");
    conf.put("yarn.scheduler.capacity.root.queues", "default, test_1, test_2");
    conf.put("yarn.scheduler.capacity.root.test_1.queues", "test_1_1, test_1_2, test_1_3");
    conf.put("yarn.scheduler.capacity.root.default.capacity", "1w");
    conf.put("yarn.scheduler.capacity.root.test_1.capacity", "[memory=16384, vcores=16]");
    conf.put("yarn.scheduler.capacity.root.test_2.capacity", "3w");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_1.capacity", "[memory=2048, vcores=2]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_2.capacity", "[memory=2048, vcores=2]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_3.capacity", "1w");
    runTest(createConfiguration(conf));
  }

  @Test
  public void testSchedulerAbsoluteAndWeightUsingCapacityVector()
      throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.legacy-queue-mode.enabled", "false");
    conf.put("yarn.scheduler.capacity.root.queues", "default, test_1, test_2");
    conf.put("yarn.scheduler.capacity.root.test_1.queues", "test_1_1, test_1_2, test_1_3");
    conf.put("yarn.scheduler.capacity.root.default.capacity", "[memory=1w, vcores=1w]");
    conf.put("yarn.scheduler.capacity.root.test_1.capacity", "[memory=16384, vcores=16]");
    conf.put("yarn.scheduler.capacity.root.test_2.capacity", "[memory=3w, vcores=3w]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_1.capacity", "[memory=2048, vcores=2]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_2.capacity", "[memory=2048, vcores=2]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_3.capacity", "[memory=1w, vcores=1w]");
    runTest("testSchedulerAbsoluteAndWeight", createConfiguration(conf));
  }

  @Test
  public void testSchedulerAbsoluteAndPercentageAndWeight()
      throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.legacy-queue-mode.enabled", "false");
    conf.put("yarn.scheduler.capacity.root.queues", "default, test_1, test_2");
    conf.put("yarn.scheduler.capacity.root.test_1.queues", "test_1_1, test_1_2, test_1_3");
    conf.put("yarn.scheduler.capacity.root.default.capacity", "1w");
    conf.put("yarn.scheduler.capacity.root.test_1.capacity", "[memory=16384, vcores=16]");
    conf.put("yarn.scheduler.capacity.root.test_2.capacity", "75");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_1.capacity", "50");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_2.capacity", "1w");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_3.capacity", "[memory=12288, vcores=12]");
    runTest(createConfiguration(conf));
  }

  @Test
  public void testSchedulerAbsoluteAndPercentageAndWeightUsingCapacityVector()
      throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.legacy-queue-mode.enabled", "false");
    conf.put("yarn.scheduler.capacity.root.queues", "default, test_1, test_2");
    conf.put("yarn.scheduler.capacity.root.test_1.queues", "test_1_1, test_1_2, test_1_3");
    conf.put("yarn.scheduler.capacity.root.default.capacity", "[memory=1w, vcores=1w]");
    conf.put("yarn.scheduler.capacity.root.test_1.capacity", "[memory=16384, vcores=16]");
    conf.put("yarn.scheduler.capacity.root.test_2.capacity", "[memory=75%, vcores=75%]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_1.capacity", "[memory=50%, vcores=50%]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_2.capacity", "[memory=1w, vcores=1w]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_3.capacity", "[memory=12288, vcores=12]");
    runTest("testSchedulerAbsoluteAndPercentageAndWeight", createConfiguration(conf));
  }

  @Test
  public void testSchedulerAbsoluteAndPercentageAndWeightMixed()
      throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.legacy-queue-mode.enabled", "false");
    conf.put("yarn.scheduler.capacity.root.queues", "default, test_1, test_2");
    conf.put("yarn.scheduler.capacity.root.test_1.queues", "test_1_1, test_1_2, test_1_3");
    conf.put("yarn.scheduler.capacity.root.default.capacity", "[memory=1w, vcores=4]");
    conf.put("yarn.scheduler.capacity.root.test_1.capacity", "[memory=16384, vcores=100%]");
    conf.put("yarn.scheduler.capacity.root.test_2.capacity", "[memory=3w, vcores=12]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_1.capacity", "[memory=1w, vcores=1w]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_2.capacity", "[memory=50%, vcores=2]");
    conf.put("yarn.scheduler.capacity.root.test_1.test_1_3.capacity", "[memory=12288, vcores=86%]");
    runTest(createConfiguration(conf));
  }

  private void runTest(Configuration configuration) throws Exception {
    final String testMethod = Thread.currentThread()
        .getStackTrace()[2].getMethodName();
    runTest(testMethod, configuration);
  }

  private void runTest(String name, Configuration configuration) throws Exception {
    initResourceManager(configuration);

    assertJsonResponse(sendRequest(), String.format(EXPECTED_FILE_TMPL, name, 0));

    rm.registerNode("n1:1234", 16384, 16);
    rm.registerNode("n2:1234", 16384, 16);
    assertJsonResponse(sendRequest(), String.format(EXPECTED_FILE_TMPL, name, 32));

    MockNM mockNM = rm.registerNode("n3:1234", 32768, 32);
    assertJsonResponse(sendRequest(), String.format(EXPECTED_FILE_TMPL, name, 64));

    rm.unRegisterNode(mockNM);
    assertJsonResponse(sendRequest(), String.format(EXPECTED_FILE_TMPL, name, 32));
  }

  private ClientResponse sendRequest() {
    return resource().path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
  }

  private void initResourceManager(Configuration conf) throws IOException {
    rm = createMockRM(new CapacitySchedulerConfiguration(conf));
    GuiceServletConfig.setInjector(
        Guice.createInjector(new TestRMWebServicesCapacitySched.WebServletModule(rm)));
    rm.start();
  }
}
