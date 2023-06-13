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

public class TestRMWebServicesCapacitySchedulerMixedMode extends JerseyTestBase {

  private static final String EXPECTED_FILE_TMPL = "webapp/mixed-%s-%d.json";

  private MockRM rm;

  public TestRMWebServicesCapacitySchedulerMixedMode() {
    super(createWebAppDescriptor());
  }


  /*
   * root.default                 1/8       [memory=4096,   vcores=4]  [memory=1w, vcores=1w] [memory=12.5%, vcores=12.5%]
   * root.test_1                  4/8       [memory=16384, vcores=16]  [memory=4w, vcores=4w] [memory=50%,   vcores=50%]
   * root.test_1.test_1_1           2/8       [memory=2048, vcores=2]  [memory=2w, vcores=2w] [memory=25%,   vcores=25%]
   * root.test_1.test_1_2           2/8       [memory=2048, vcores=2]  [memory=2w, vcores=2w] [memory=25%,   vcores=25%]
   * root.test_1.test_1_3           4/8       [memory=8192, vcores=8]  [memory=4w, vcores=4w] [memory=50%,   vcores=50%]
   * root.test_2                  2/8       [memory=8192,   vcores=8]  [memory=2w, vcores=2w] [memory=25%,   vcores=25%]
   * 32GB 32Core
   */


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

    // todo absoluteCapacity calc

    /*                          effectiveMin (32GB 32VCores)
     * root.default             [memory=4096,  vcores=4]
     * root.test_1              [memory=50%,   vcores=50%]
     * root.test_1.test_1_1     [memory=2048,  vcores=2]    todo why am limit is 4096, 1 ?
     * root.test_1.test_1_2     [memory=2048,  vcores=2]
     * root.test_1.test_1_3     [memory=12288, vcores=12]
     * root.test_2              [memory=12288, vcores=12]
     */
    runTest(createConfiguration(conf));
  }

  private void runTest(Configuration configuration) throws Exception {
    final String testMethod = Thread.currentThread()
        .getStackTrace()[2].getMethodName();

    initResourceManager(configuration);

    assertJsonResponse(sendRequest(), String.format(EXPECTED_FILE_TMPL, testMethod, 0));

    rm.registerNode("n1:1234", 16384, 16);
    rm.registerNode("n2:1234", 16384, 16);
    assertJsonResponse(sendRequest(), String.format(EXPECTED_FILE_TMPL, testMethod, 32));

    MockNM mockNM = rm.registerNode("n3:1234", 32768, 32);
    assertJsonResponse(sendRequest(), String.format(EXPECTED_FILE_TMPL, testMethod, 64));

    rm.unRegisterNode(mockNM);
    assertJsonResponse(sendRequest(), String.format(EXPECTED_FILE_TMPL, testMethod, 32));
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
