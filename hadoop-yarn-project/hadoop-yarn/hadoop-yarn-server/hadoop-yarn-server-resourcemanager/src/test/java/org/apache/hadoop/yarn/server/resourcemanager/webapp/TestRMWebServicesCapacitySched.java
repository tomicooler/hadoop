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

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.ClientResponse;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.assertJsonResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.assertJsonType;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.assertXmlResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createRM;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createWebAppDescriptor;
import static org.junit.Assert.assertEquals;

public class TestRMWebServicesCapacitySched extends JerseyTestBase {

  public TestRMWebServicesCapacitySched() {
    super(createWebAppDescriptor());
  }

  @Test
  public void testClusterScheduler() throws Exception {
    try (MockRM rm = createRM(createConfig())){
      assertJsonResponse(resource().path("ws/v1/cluster/scheduler")
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class),
          "webapp/scheduler-response.json");
      assertJsonResponse(resource().path("ws/v1/cluster/scheduler/")
              .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class),
          "webapp/scheduler-response.json");
      assertJsonResponse(resource().path("ws/v1/cluster/scheduler")
              .get(ClientResponse.class)
          , "webapp/scheduler-response.json");
      assertXmlResponse(resource().path("ws/v1/cluster/scheduler/")
              .accept(MediaType.APPLICATION_XML).get(ClientResponse.class),
          "webapp/scheduler-response.xml");
    }
  }

  @Test
  public void testPerUserResources() throws Exception {
    try (MockRM rm = createRM(createConfig())){
      rm.registerNode("h1:1234", 10 * GB, 10);
      MockRMAppSubmitter.submit(rm, MockRMAppSubmissionData.Builder
          .createWithMemory(10, rm)
          .withAppName("app1")
          .withUser("user1")
          .withAcls(null)
          .withQueue("a")
          .withUnmanagedAM(false)
          .build()
      );
      MockRMAppSubmitter.submit(rm, MockRMAppSubmissionData.Builder
          .createWithMemory(20, rm)
          .withAppName("app2")
          .withUser("user2")
          .withAcls(null)
          .withQueue("b")
          .withUnmanagedAM(false)
          .build()
      );
      assertXmlResponse(resource().path("ws/v1/cluster/scheduler")
              .accept(MediaType.APPLICATION_XML).get(ClientResponse.class),
          "webapp/scheduler-response-PerUserResources.xml");
      assertJsonResponse(resource().path("ws/v1/cluster/scheduler")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class),
          "webapp/scheduler-response-PerUserResources.json");

    }
  }

  @Test
  public void testNodeLabelDefaultAPI() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(createConfig());
    conf.setDefaultNodeLabelExpression("root", "ROOT-INHERITED");
    conf.setDefaultNodeLabelExpression("root.a", "root-a-default-label");
    try (MockRM rm = createRM(conf)) {
      ClientResponse response = resource().path("ws/v1/cluster/scheduler")
          .accept(MediaType.APPLICATION_XML).get(ClientResponse.class);
      assertXmlResponse(response, "webapp/scheduler-response-NodeLabelDefaultAPI.xml");
    }
  }
  @Test
  public void testClusterSchedulerOverviewCapacity() throws Exception {
    try (MockRM rm = createRM(createConfig())) {
      ClientResponse response = resource().path("ws/v1/cluster/scheduler-overview")
          .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
      assertJsonType(response);
      JSONObject json = response.getEntity(JSONObject.class);
      TestRMWebServices.verifyClusterSchedulerOverView(json, "Capacity Scheduler");
    }
  }

  @Test
  public void testResourceInfo() {
    Resource res = Resources.createResource(10, 1);
    // If we add a new resource (e.g. disks), then
    // CapacitySchedulerPage and these RM WebServices + docs need to be updated
    // e.g. ResourceInfo
    assertEquals("<memory:10, vCores:1>", res.toString());
  }

  private Configuration createConfig() {
    Configuration conf = new Configuration();
    conf.set("yarn.scheduler.capacity.root.queues", "a, b, c");
    conf.set("yarn.scheduler.capacity.root.a.capacity", "20");
    conf.set("yarn.scheduler.capacity.root.a.maximum-capacity", "50");
    conf.set("yarn.scheduler.capacity.root.a.max-parallel-app", "42");
    conf.set("yarn.scheduler.capacity.root.b.capacity", "70");
    conf.set("yarn.scheduler.capacity.root.c.capacity", "10");
    return conf;
  }
}
