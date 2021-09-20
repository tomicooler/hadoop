/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.Task;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.MockNM.createMockNodeStatus;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A1;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.A2;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.B1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCapacitySchedulerAppManagement extends CapacitySchedulerTestBase {

  @Test
  public void testGetAppsInQueue() throws Exception {
    Application application_0 = new Application("user_0", "a1", resourceManager);
    application_0.submit();

    Application application_1 = new Application("user_0", "a2", resourceManager);
    application_1.submit();

    Application application_2 = new Application("user_0", "b2", resourceManager);
    application_2.submit();

    ResourceScheduler scheduler = resourceManager.getResourceScheduler();

    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(application_0.getApplicationAttemptId()));
    assertTrue(appsInA.contains(application_1.getApplicationAttemptId()));
    assertEquals(2, appsInA.size());

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(application_0.getApplicationAttemptId()));
    assertTrue(appsInRoot.contains(application_1.getApplicationAttemptId()));
    assertTrue(appsInRoot.contains(application_2.getApplicationAttemptId()));
    assertEquals(3, appsInRoot.size());

    Assert.assertNull(scheduler.getAppsInQueue("nonexistentqueue"));
  }

  @Test
  public void testAddAndRemoveAppFromCapacityScheduler() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    CapacitySchedulerQueueHelpers.setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    @SuppressWarnings("unchecked")
    AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode> cs =
        (AbstractYarnScheduler<SchedulerApplicationAttempt, SchedulerNode>) rm
            .getResourceScheduler();
    SchedulerApplication<SchedulerApplicationAttempt> app =
        TestSchedulerUtils.verifyAppAddedAndRemovedFromScheduler(
            cs.getSchedulerApplications(), cs, "a1");
    Assert.assertEquals("a1", app.getQueue().getQueueName());
  }

  @Test
  public void testKillAllAppsInQueue() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();

    // submit an app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());
    String queue =
        scheduler.getApplicationAttempt(appsInA1.get(0)).getQueue()
            .getQueueName();
    Assert.assertEquals("a1", queue);

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    // now kill the app
    scheduler.killAllAppsInQueue("a1");

    // check postconditions
    rm.waitForState(app.getApplicationId(), RMAppState.KILLED);
    rm.waitForAppRemovedFromScheduler(app.getApplicationId());
    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.isEmpty());

    appsInA1 = scheduler.getAppsInQueue("a1");
    assertTrue(appsInA1.isEmpty());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.isEmpty());

    rm.stop();
  }

  @Test
  public void testKillAllAppsInvalidSource() throws Exception {
    MockRM rm = setUpMove();
    YarnScheduler scheduler = rm.getResourceScheduler();

    // submit an app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    // now kill the app
    try {
      scheduler.killAllAppsInQueue("DOES_NOT_EXIST");
      Assert.fail();
    } catch (YarnException e) {
      // expected
    }

    // check postconditions, app should still be in a1
    appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    rm.stop();
  }

  @Test
  public void testMoveAllApps() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();

    // submit an app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());
    String queue =
        scheduler.getApplicationAttempt(appsInA1.get(0)).getQueue()
            .getQueueName();
    Assert.assertEquals("a1", queue);

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    List<ApplicationAttemptId> appsInB1 = scheduler.getAppsInQueue("b1");
    assertTrue(appsInB1.isEmpty());

    List<ApplicationAttemptId> appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.isEmpty());

    // now move the app
    scheduler.moveAllApps("a1", "b1");

    // check postconditions
    Thread.sleep(1000);
    appsInB1 = scheduler.getAppsInQueue("b1");
    assertEquals(1, appsInB1.size());
    queue =
        scheduler.getApplicationAttempt(appsInB1.get(0)).getQueue()
            .getQueueName();
    Assert.assertEquals("b1", queue);

    appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.contains(appAttemptId));
    assertEquals(1, appsInB.size());

    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    appsInA1 = scheduler.getAppsInQueue("a1");
    assertTrue(appsInA1.isEmpty());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.isEmpty());

    rm.stop();
  }

  @Test
  public void testMoveAllAppsInvalidDestination() throws Exception {
    MockRM rm = setUpMove();
    YarnScheduler scheduler = rm.getResourceScheduler();

    // submit an app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    List<ApplicationAttemptId> appsInB1 = scheduler.getAppsInQueue("b1");
    assertTrue(appsInB1.isEmpty());

    List<ApplicationAttemptId> appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.isEmpty());

    // now move the app
    try {
      scheduler.moveAllApps("a1", "DOES_NOT_EXIST");
      Assert.fail();
    } catch (YarnException e) {
      // expected
    }

    // check postconditions, app should still be in a1
    appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    appsInB1 = scheduler.getAppsInQueue("b1");
    assertTrue(appsInB1.isEmpty());

    appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.isEmpty());

    rm.stop();
  }

  @Test
  public void testMoveAllAppsInvalidSource() throws Exception {
    MockRM rm = setUpMove();
    YarnScheduler scheduler = rm.getResourceScheduler();

    // submit an app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    List<ApplicationAttemptId> appsInB1 = scheduler.getAppsInQueue("b1");
    assertTrue(appsInB1.isEmpty());

    List<ApplicationAttemptId> appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.isEmpty());

    // now move the app
    try {
      scheduler.moveAllApps("DOES_NOT_EXIST", "b1");
      Assert.fail();
    } catch (YarnException e) {
      // expected
    }

    // check postconditions, app should still be in a1
    appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    appsInB1 = scheduler.getAppsInQueue("b1");
    assertTrue(appsInB1.isEmpty());

    appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.isEmpty());

    rm.stop();
  }

  @Test(timeout = 60000)
  public void testMoveAttemptNotAdded() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(getCapacityConfiguration(conf));
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    ApplicationId appId = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);

    RMAppAttemptMetrics attemptMetric =
        new RMAppAttemptMetrics(appAttemptId, rm.getRMContext());
    RMAppImpl app = mock(RMAppImpl.class);
    when(app.getApplicationId()).thenReturn(appId);
    RMAppAttemptImpl attempt = mock(RMAppAttemptImpl.class);
    Container container = mock(Container.class);
    when(attempt.getMasterContainer()).thenReturn(container);
    ApplicationSubmissionContext submissionContext =
        mock(ApplicationSubmissionContext.class);
    when(attempt.getSubmissionContext()).thenReturn(submissionContext);
    when(attempt.getAppAttemptId()).thenReturn(appAttemptId);
    when(attempt.getRMAppAttemptMetrics()).thenReturn(attemptMetric);
    when(app.getCurrentAppAttempt()).thenReturn(attempt);

    rm.getRMContext().getRMApps().put(appId, app);

    SchedulerEvent addAppEvent =
        new AppAddedSchedulerEvent(appId, "a1", "user");
    try {
      cs.moveApplication(appId, "b1");
      fail("Move should throw exception app not available");
    } catch (YarnException e) {
      assertEquals("App to be moved application_100_0001 not found.",
          e.getMessage());
    }
    cs.handle(addAppEvent);
    cs.moveApplication(appId, "b1");
    SchedulerEvent addAttemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    cs.handle(addAttemptEvent);
    CSQueue rootQ = cs.getRootQueue();
    CSQueue queueB = cs.getQueue("b");
    CSQueue queueA = cs.getQueue("a");
    CSQueue queueA1 = cs.getQueue("a1");
    CSQueue queueB1 = cs.getQueue("b1");
    Assert.assertEquals(1, rootQ.getNumApplications());
    Assert.assertEquals(0, queueA.getNumApplications());
    Assert.assertEquals(1, queueB.getNumApplications());
    Assert.assertEquals(0, queueA1.getNumApplications());
    Assert.assertEquals(1, queueB1.getNumApplications());

    rm.close();
  }

  @Test
  public void testRemoveAttemptMoveAdded() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        CapacityScheduler.class);
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
    // Create Mock RM
    MockRM rm = new MockRM(getCapacityConfiguration(conf));
    CapacityScheduler sch = (CapacityScheduler) rm.getResourceScheduler();
    // add node
    Resource newResource = Resource.newInstance(4 * GB, 1);
    RMNode node = MockNodes.newNodeInfo(0, newResource, 1, "127.0.0.1");
    SchedulerEvent addNode = new NodeAddedSchedulerEvent(node);
    sch.handle(addNode);

    ApplicationAttemptId appAttemptId = appHelper(rm, sch, 100, 1, "a1", "user");

    // get Queues
    CSQueue queueA1 = sch.getQueue("a1");
    CSQueue queueB = sch.getQueue("b");
    CSQueue queueB1 = sch.getQueue("b1");

    // add Running rm container and simulate live containers to a1
    ContainerId newContainerId = ContainerId.newContainerId(appAttemptId, 2);
    RMContainerImpl rmContainer = mock(RMContainerImpl.class);
    when(rmContainer.getState()).thenReturn(RMContainerState.RUNNING);
    Container container2 = mock(Container.class);
    when(rmContainer.getContainer()).thenReturn(container2);
    Resource resource = Resource.newInstance(1024, 1);
    when(container2.getResource()).thenReturn(resource);
    when(rmContainer.getExecutionType()).thenReturn(ExecutionType.GUARANTEED);
    when(container2.getNodeId()).thenReturn(node.getNodeID());
    when(container2.getId()).thenReturn(newContainerId);
    when(rmContainer.getNodeLabelExpression())
        .thenReturn(RMNodeLabelsManager.NO_LABEL);
    when(rmContainer.getContainerId()).thenReturn(newContainerId);
    sch.getApplicationAttempt(appAttemptId).getLiveContainersMap()
        .put(newContainerId, rmContainer);
    QueueMetrics queueA1M = queueA1.getMetrics();
    queueA1M.incrPendingResources(rmContainer.getNodeLabelExpression(),
        "user1", 1, resource);
    queueA1M.allocateResources(rmContainer.getNodeLabelExpression(),
        "user1", resource);
    // remove attempt
    sch.handle(new AppAttemptRemovedSchedulerEvent(appAttemptId,
        RMAppAttemptState.KILLED, true));
    // Move application to queue b1
    sch.moveApplication(appAttemptId.getApplicationId(), "b1");
    // Check queue metrics after move
    Assert.assertEquals(0, queueA1.getNumApplications());
    Assert.assertEquals(1, queueB.getNumApplications());
    Assert.assertEquals(0, queueB1.getNumApplications());

    // Release attempt add event
    ApplicationAttemptId appAttemptId2 =
        BuilderUtils.newApplicationAttemptId(appAttemptId.getApplicationId(), 2);
    SchedulerEvent addAttemptEvent2 =
        new AppAttemptAddedSchedulerEvent(appAttemptId2, true);
    sch.handle(addAttemptEvent2);

    // Check metrics after attempt added
    Assert.assertEquals(0, queueA1.getNumApplications());
    Assert.assertEquals(1, queueB.getNumApplications());
    Assert.assertEquals(1, queueB1.getNumApplications());


    QueueMetrics queueB1M = queueB1.getMetrics();
    QueueMetrics queueBM = queueB.getMetrics();
    // Verify allocation MB of current state
    Assert.assertEquals(0, queueA1M.getAllocatedMB());
    Assert.assertEquals(0, queueA1M.getAllocatedVirtualCores());
    Assert.assertEquals(1024, queueB1M.getAllocatedMB());
    Assert.assertEquals(1, queueB1M.getAllocatedVirtualCores());

    // remove attempt
    sch.handle(new AppAttemptRemovedSchedulerEvent(appAttemptId2,
        RMAppAttemptState.FINISHED, false));

    Assert.assertEquals(0, queueA1M.getAllocatedMB());
    Assert.assertEquals(0, queueA1M.getAllocatedVirtualCores());
    Assert.assertEquals(0, queueB1M.getAllocatedMB());
    Assert.assertEquals(0, queueB1M.getAllocatedVirtualCores());

    verifyQueueMetrics(queueB1M);
    verifyQueueMetrics(queueBM);
    // Verify queue A1 metrics
    verifyQueueMetrics(queueA1M);
    rm.close();
  }

  @Test
  public void testMoveAppBasic() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();
    QueueMetrics metrics = scheduler.getRootQueueMetrics();
    Assert.assertEquals(0, metrics.getAppsPending());
    // submit an app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();
    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());
    String queue =
        scheduler.getApplicationAttempt(appsInA1.get(0)).getQueue()
            .getQueueName();
    Assert.assertEquals("a1", queue);

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    assertEquals(1, metrics.getAppsPending());

    List<ApplicationAttemptId> appsInB1 = scheduler.getAppsInQueue("b1");
    assertTrue(appsInB1.isEmpty());

    List<ApplicationAttemptId> appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.isEmpty());

    // now move the app
    scheduler.moveApplication(app.getApplicationId(), "b1");

    // check postconditions
    appsInB1 = scheduler.getAppsInQueue("b1");
    assertEquals(1, appsInB1.size());
    queue =
        scheduler.getApplicationAttempt(appsInB1.get(0)).getQueue()
            .getQueueName();
    Assert.assertEquals("b1", queue);

    appsInB = scheduler.getAppsInQueue("b");
    assertTrue(appsInB.contains(appAttemptId));
    assertEquals(1, appsInB.size());

    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    assertEquals(1, metrics.getAppsPending());

    appsInA1 = scheduler.getAppsInQueue("a1");
    assertTrue(appsInA1.isEmpty());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.isEmpty());

    rm.stop();
  }

  @Test
  public void testMoveAppPendingMetrics() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();
    QueueMetrics metrics = scheduler.getRootQueueMetrics();
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    List<ApplicationAttemptId> appsInB1 = scheduler.getAppsInQueue("b1");

    assertEquals(0, appsInA1.size());
    assertEquals(0, appsInB1.size());
    Assert.assertEquals(0, metrics.getAppsPending());

    // submit two apps in a1
    RMApp app1 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("a1")
            .build());
    RMApp app2 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-2")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("a1")
            .build());

    appsInA1 = scheduler.getAppsInQueue("a1");
    appsInB1 = scheduler.getAppsInQueue("b1");
    assertEquals(2, appsInA1.size());
    assertEquals(0, appsInB1.size());
    assertEquals(2, metrics.getAppsPending());

    // submit one app in b1
    RMApp app3 = MockRMAppSubmitter.submit(rm,
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-2")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("b1")
            .build());

    appsInA1 = scheduler.getAppsInQueue("a1");
    appsInB1 = scheduler.getAppsInQueue("b1");
    assertEquals(2, appsInA1.size());
    assertEquals(1, appsInB1.size());
    assertEquals(3, metrics.getAppsPending());

    // now move the app1 from a1 to b1
    scheduler.moveApplication(app1.getApplicationId(), "b1");

    appsInA1 = scheduler.getAppsInQueue("a1");
    appsInB1 = scheduler.getAppsInQueue("b1");
    assertEquals(1, appsInA1.size());
    assertEquals(2, appsInB1.size());
    assertEquals(3, metrics.getAppsPending());

    // now move the app2 from a1 to b1
    scheduler.moveApplication(app2.getApplicationId(), "b1");

    appsInA1 = scheduler.getAppsInQueue("a1");
    appsInB1 = scheduler.getAppsInQueue("b1");
    assertEquals(0, appsInA1.size());
    assertEquals(3, appsInB1.size());
    assertEquals(3, metrics.getAppsPending());

    // now move the app3 from b1 to a1
    scheduler.moveApplication(app3.getApplicationId(), "a1");

    appsInA1 = scheduler.getAppsInQueue("a1");
    appsInB1 = scheduler.getAppsInQueue("b1");
    assertEquals(1, appsInA1.size());
    assertEquals(2, appsInB1.size());
    assertEquals(3, metrics.getAppsPending());
    rm.stop();
  }

  @Test
  public void testMoveAppSameParent() throws Exception {
    MockRM rm = setUpMove();
    AbstractYarnScheduler scheduler =
        (AbstractYarnScheduler) rm.getResourceScheduler();

    // submit an app
    MockRMAppSubmissionData data =
        MockRMAppSubmissionData.Builder.createWithMemory(GB, rm)
            .withAppName("test-move-1")
            .withUser("user_0")
            .withAcls(null)
            .withQueue("a1")
            .withUnmanagedAM(false)
            .build();
    RMApp app = MockRMAppSubmitter.submit(rm, data);
    ApplicationAttemptId appAttemptId =
        rm.getApplicationReport(app.getApplicationId())
            .getCurrentApplicationAttemptId();

    // check preconditions
    List<ApplicationAttemptId> appsInA1 = scheduler.getAppsInQueue("a1");
    assertEquals(1, appsInA1.size());
    String queue =
        scheduler.getApplicationAttempt(appsInA1.get(0)).getQueue()
            .getQueueName();
    Assert.assertEquals("a1", queue);

    List<ApplicationAttemptId> appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    List<ApplicationAttemptId> appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    List<ApplicationAttemptId> appsInA2 = scheduler.getAppsInQueue("a2");
    assertTrue(appsInA2.isEmpty());

    // now move the app
    scheduler.moveApplication(app.getApplicationId(), "a2");

    // check postconditions
    appsInA2 = scheduler.getAppsInQueue("a2");
    assertEquals(1, appsInA2.size());
    queue =
        scheduler.getApplicationAttempt(appsInA2.get(0)).getQueue()
            .getQueueName();
    Assert.assertEquals("a2", queue);

    appsInA1 = scheduler.getAppsInQueue("a1");
    assertTrue(appsInA1.isEmpty());

    appsInA = scheduler.getAppsInQueue("a");
    assertTrue(appsInA.contains(appAttemptId));
    assertEquals(1, appsInA.size());

    appsInRoot = scheduler.getAppsInQueue("root");
    assertTrue(appsInRoot.contains(appAttemptId));
    assertEquals(1, appsInRoot.size());

    rm.stop();
  }

  @Test
  public void testMoveAppForMoveToQueueWithFreeCap() throws Exception {

    ResourceScheduler scheduler = resourceManager.getResourceScheduler();

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register node1
    String host_0 = "host_0";
    NodeManager nm_0 =
        registerNode(resourceManager, host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(4 * GB, 1), mockNodeStatus);

    // Register node2
    String host_1 = "host_1";
    NodeManager nm_1 =
        registerNode(resourceManager, host_1, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(2 * GB, 1), mockNodeStatus);

    // ResourceRequest priorities
    Priority priority_0 = Priority.newInstance(0);
    Priority priority_1 = Priority.newInstance(1);

    // Submit application_0
    Application application_0 =
        new Application("user_0", "a1", resourceManager);
    application_0.submit(); // app + app attempt event sent to scheduler

    application_0.addNodeManager(host_0, 1234, nm_0);
    application_0.addNodeManager(host_1, 1234, nm_1);

    Resource capability_0_0 = Resources.createResource(1 * GB, 1);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);

    Resource capability_0_1 = Resources.createResource(2 * GB, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 =
        new Task(application_0, priority_1, new String[]{host_0, host_1});
    application_0.addTask(task_0_0);

    // Submit application_1
    Application application_1 =
        new Application("user_1", "b2", resourceManager);
    application_1.submit(); // app + app attempt event sent to scheduler

    application_1.addNodeManager(host_0, 1234, nm_0);
    application_1.addNodeManager(host_1, 1234, nm_1);

    Resource capability_1_0 = Resources.createResource(1 * GB, 1);
    application_1.addResourceRequestSpec(priority_1, capability_1_0);

    Resource capability_1_1 = Resources.createResource(2 * GB, 1);
    application_1.addResourceRequestSpec(priority_0, capability_1_1);

    Task task_1_0 =
        new Task(application_1, priority_1, new String[]{host_0, host_1});
    application_1.addTask(task_1_0);

    // Send resource requests to the scheduler
    application_0.schedule(); // allocate
    application_1.schedule(); // allocate

    // task_0_0 task_1_0 allocated, used=2G
    nodeUpdate(resourceManager, nm_0);

    // nothing allocated
    nodeUpdate(resourceManager, nm_1);

    // Get allocations from the scheduler
    application_0.schedule(); // task_0_0
    checkApplicationResourceUsage(1 * GB, application_0);

    application_1.schedule(); // task_1_0
    checkApplicationResourceUsage(1 * GB, application_1);

    checkNodeResourceUsage(2 * GB, nm_0); // task_0_0 (1G) and task_1_0 (1G) 2G
    // available
    checkNodeResourceUsage(0 * GB, nm_1); // no tasks, 2G available

    // move app from a1(30% cap of total 10.5% cap) to b1(79,2% cap of 89,5%
    // total cap)
    scheduler.moveApplication(application_0.getApplicationId(), "b1");

    // 2GB 1C
    Task task_1_1 =
        new Task(application_1, priority_0,
            new String[]{ResourceRequest.ANY});
    application_1.addTask(task_1_1);

    application_1.schedule();

    // 2GB 1C
    Task task_0_1 =
        new Task(application_0, priority_0, new String[]{host_0, host_1});
    application_0.addTask(task_0_1);

    application_0.schedule();

    // prev 2G used free 2G
    nodeUpdate(resourceManager, nm_0);

    // prev 0G used free 2G
    nodeUpdate(resourceManager, nm_1);

    // Get allocations from the scheduler
    application_1.schedule();
    checkApplicationResourceUsage(3 * GB, application_1);

    // Get allocations from the scheduler
    application_0.schedule();
    checkApplicationResourceUsage(3 * GB, application_0);

    checkNodeResourceUsage(4 * GB, nm_0);
    checkNodeResourceUsage(2 * GB, nm_1);

  }

  @Test
  public void testMoveAppSuccess() throws Exception {

    ResourceScheduler scheduler = resourceManager.getResourceScheduler();

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register node1
    String host_0 = "host_0";
    NodeManager nm_0 =
        registerNode(resourceManager, host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(5 * GB, 1), mockNodeStatus);

    // Register node2
    String host_1 = "host_1";
    NodeManager nm_1 =
        registerNode(resourceManager, host_1, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(5 * GB, 1), mockNodeStatus);

    // ResourceRequest priorities
    Priority priority_0 = Priority.newInstance(0);
    Priority priority_1 = Priority.newInstance(1);

    // Submit application_0
    Application application_0 =
        new Application("user_0", "a1", resourceManager);
    application_0.submit(); // app + app attempt event sent to scheduler

    application_0.addNodeManager(host_0, 1234, nm_0);
    application_0.addNodeManager(host_1, 1234, nm_1);

    Resource capability_0_0 = Resources.createResource(3 * GB, 1);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);

    Resource capability_0_1 = Resources.createResource(2 * GB, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 =
        new Task(application_0, priority_1, new String[]{host_0, host_1});
    application_0.addTask(task_0_0);

    // Submit application_1
    Application application_1 =
        new Application("user_1", "b2", resourceManager);
    application_1.submit(); // app + app attempt event sent to scheduler

    application_1.addNodeManager(host_0, 1234, nm_0);
    application_1.addNodeManager(host_1, 1234, nm_1);

    Resource capability_1_0 = Resources.createResource(1 * GB, 1);
    application_1.addResourceRequestSpec(priority_1, capability_1_0);

    Resource capability_1_1 = Resources.createResource(2 * GB, 1);
    application_1.addResourceRequestSpec(priority_0, capability_1_1);

    Task task_1_0 =
        new Task(application_1, priority_1, new String[]{host_0, host_1});
    application_1.addTask(task_1_0);

    // Send resource requests to the scheduler
    application_0.schedule(); // allocate
    application_1.schedule(); // allocate

    // b2 can only run 1 app at a time
    scheduler.moveApplication(application_0.getApplicationId(), "b2");

    nodeUpdate(resourceManager, nm_0);

    nodeUpdate(resourceManager, nm_1);

    // Get allocations from the scheduler
    application_0.schedule(); // task_0_0
    checkApplicationResourceUsage(0 * GB, application_0);

    application_1.schedule(); // task_1_0
    checkApplicationResourceUsage(1 * GB, application_1);

    // task_1_0 (1G) application_0 moved to b2 with max running app 1 so it is
    // not scheduled
    checkNodeResourceUsage(1 * GB, nm_0);
    checkNodeResourceUsage(0 * GB, nm_1);

    // lets move application_0 to a queue where it can run
    scheduler.moveApplication(application_0.getApplicationId(), "a2");
    application_0.schedule();

    nodeUpdate(resourceManager, nm_1);

    // Get allocations from the scheduler
    application_0.schedule(); // task_0_0
    checkApplicationResourceUsage(3 * GB, application_0);

    checkNodeResourceUsage(1 * GB, nm_0);
    checkNodeResourceUsage(3 * GB, nm_1);

  }

  @Test(expected = YarnException.class)
  public void testMoveAppViolateQueueState() throws Exception {
    resourceManager = new ResourceManager() {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
        mgr.init(getConfig());
        return mgr;
      }
    };
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    CapacitySchedulerQueueHelpers.setupQueueConfiguration(csConf);
    StringBuilder qState = new StringBuilder();
    qState.append(CapacitySchedulerConfiguration.PREFIX).append(B)
        .append(CapacitySchedulerConfiguration.DOT)
        .append(CapacitySchedulerConfiguration.STATE);
    csConf.set(qState.toString(), QueueState.STOPPED.name());
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    resourceManager.init(conf);
    resourceManager.getRMContext().getContainerTokenSecretManager()
        .rollMasterKey();
    resourceManager.getRMContext().getNMTokenSecretManager().rollMasterKey();
    ((AsyncDispatcher) resourceManager.getRMContext().getDispatcher()).start();
    mockContext = mock(RMContext.class);
    when(mockContext.getConfigurationProvider()).thenReturn(
        new LocalConfigurationProvider());

    ResourceScheduler scheduler = resourceManager.getResourceScheduler();

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register node1
    String host_0 = "host_0";
    NodeManager nm_0 =
        registerNode(resourceManager, host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(6 * GB, 1), mockNodeStatus);

    // ResourceRequest priorities
    Priority priority_0 = Priority.newInstance(0);
    Priority priority_1 = Priority.newInstance(1);

    // Submit application_0
    Application application_0 =
        new Application("user_0", "a1", resourceManager);
    application_0.submit(); // app + app attempt event sent to scheduler

    application_0.addNodeManager(host_0, 1234, nm_0);

    Resource capability_0_0 = Resources.createResource(3 * GB, 1);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);

    Resource capability_0_1 = Resources.createResource(2 * GB, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 =
        new Task(application_0, priority_1, new String[]{host_0});
    application_0.addTask(task_0_0);

    // Send resource requests to the scheduler
    application_0.schedule(); // allocate

    // task_0_0 allocated
    nodeUpdate(resourceManager, nm_0);

    // Get allocations from the scheduler
    application_0.schedule(); // task_0_0
    checkApplicationResourceUsage(3 * GB, application_0);

    checkNodeResourceUsage(3 * GB, nm_0);
    // b2 queue contains 3GB consumption app,
    // add another 3GB will hit max capacity limit on queue b
    scheduler.moveApplication(application_0.getApplicationId(), "b1");

  }

  @Test
  public void testMoveAppQueueMetricsCheck() throws Exception {
    ResourceScheduler scheduler = resourceManager.getResourceScheduler();

    NodeStatus mockNodeStatus = createMockNodeStatus();

    // Register node1
    String host_0 = "host_0";
    NodeManager nm_0 =
        registerNode(resourceManager, host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(5 * GB, 1), mockNodeStatus);

    // Register node2
    String host_1 = "host_1";
    NodeManager nm_1 =
        registerNode(resourceManager, host_1, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(5 * GB, 1), mockNodeStatus);

    // ResourceRequest priorities
    Priority priority_0 = Priority.newInstance(0);
    Priority priority_1 = Priority.newInstance(1);

    // Submit application_0
    Application application_0 =
        new Application("user_0", "a1", resourceManager);
    application_0.submit(); // app + app attempt event sent to scheduler

    application_0.addNodeManager(host_0, 1234, nm_0);
    application_0.addNodeManager(host_1, 1234, nm_1);

    Resource capability_0_0 = Resources.createResource(3 * GB, 1);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);

    Resource capability_0_1 = Resources.createResource(2 * GB, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 =
        new Task(application_0, priority_1, new String[]{host_0, host_1});
    application_0.addTask(task_0_0);

    // Submit application_1
    Application application_1 =
        new Application("user_1", "b2", resourceManager);
    application_1.submit(); // app + app attempt event sent to scheduler

    application_1.addNodeManager(host_0, 1234, nm_0);
    application_1.addNodeManager(host_1, 1234, nm_1);

    Resource capability_1_0 = Resources.createResource(1 * GB, 1);
    application_1.addResourceRequestSpec(priority_1, capability_1_0);

    Resource capability_1_1 = Resources.createResource(2 * GB, 1);
    application_1.addResourceRequestSpec(priority_0, capability_1_1);

    Task task_1_0 =
        new Task(application_1, priority_1, new String[]{host_0, host_1});
    application_1.addTask(task_1_0);

    // Send resource requests to the scheduler
    application_0.schedule(); // allocate
    application_1.schedule(); // allocate

    nodeUpdate(resourceManager, nm_0);

    nodeUpdate(resourceManager, nm_1);

    CapacityScheduler cs =
        (CapacityScheduler) resourceManager.getResourceScheduler();
    CSQueue origRootQ = cs.getRootQueue();
    CapacitySchedulerInfo oldInfo =
        new CapacitySchedulerInfo(origRootQ, cs);
    int origNumAppsA = getNumAppsInQueue("a", origRootQ.getChildQueues());
    int origNumAppsRoot = origRootQ.getNumApplications();

    scheduler.moveApplication(application_0.getApplicationId(), "a2");

    CSQueue newRootQ = cs.getRootQueue();
    int newNumAppsA = getNumAppsInQueue("a", newRootQ.getChildQueues());
    int newNumAppsRoot = newRootQ.getNumApplications();
    CapacitySchedulerInfo newInfo =
        new CapacitySchedulerInfo(newRootQ, cs);
    CapacitySchedulerLeafQueueInfo origOldA1 =
        (CapacitySchedulerLeafQueueInfo) getQueueInfo("a1", oldInfo.getQueues());
    CapacitySchedulerLeafQueueInfo origNewA1 =
        (CapacitySchedulerLeafQueueInfo) getQueueInfo("a1", newInfo.getQueues());
    CapacitySchedulerLeafQueueInfo targetOldA2 =
        (CapacitySchedulerLeafQueueInfo) getQueueInfo("a2", oldInfo.getQueues());
    CapacitySchedulerLeafQueueInfo targetNewA2 =
        (CapacitySchedulerLeafQueueInfo) getQueueInfo("a2", newInfo.getQueues());
    // originally submitted here
    assertEquals(1, origOldA1.getNumApplications());
    assertEquals(1, origNumAppsA);
    assertEquals(2, origNumAppsRoot);
    // after the move
    assertEquals(0, origNewA1.getNumApplications());
    assertEquals(1, newNumAppsA);
    assertEquals(2, newNumAppsRoot);
    // original consumption on a1
    assertEquals(3 * GB, origOldA1.getResourcesUsed().getMemorySize());
    assertEquals(1, origOldA1.getResourcesUsed().getvCores());
    assertEquals(0, origNewA1.getResourcesUsed().getMemorySize()); // after the move
    assertEquals(0, origNewA1.getResourcesUsed().getvCores()); // after the move
    // app moved here with live containers
    assertEquals(3 * GB, targetNewA2.getResourcesUsed().getMemorySize());
    assertEquals(1, targetNewA2.getResourcesUsed().getvCores());
    // it was empty before the move
    assertEquals(0, targetOldA2.getNumApplications());
    assertEquals(0, targetOldA2.getResourcesUsed().getMemorySize());
    assertEquals(0, targetOldA2.getResourcesUsed().getvCores());
    // after the app moved here
    assertEquals(1, targetNewA2.getNumApplications());
    // 1 container on original queue before move
    assertEquals(1, origOldA1.getNumContainers());
    // after the move the resource released
    assertEquals(0, origNewA1.getNumContainers());
    // and moved to the new queue
    assertEquals(1, targetNewA2.getNumContainers());
    // which originally didn't have any
    assertEquals(0, targetOldA2.getNumContainers());
    // 1 user with 3GB
    assertEquals(3 * GB, origOldA1.getUsers().getUsersList().get(0)
        .getResourcesUsed().getMemorySize());
    // 1 user with 1 core
    assertEquals(1, origOldA1.getUsers().getUsersList().get(0)
        .getResourcesUsed().getvCores());
    // user ha no more running app in the orig queue
    assertEquals(0, origNewA1.getUsers().getUsersList().size());
    // 1 user with 3GB
    assertEquals(3 * GB, targetNewA2.getUsers().getUsersList().get(0)
        .getResourcesUsed().getMemorySize());
    // 1 user with 1 core
    assertEquals(1, targetNewA2.getUsers().getUsersList().get(0)
        .getResourcesUsed().getvCores());

    // Get allocations from the scheduler
    application_0.schedule(); // task_0_0
    checkApplicationResourceUsage(3 * GB, application_0);

    application_1.schedule(); // task_1_0
    checkApplicationResourceUsage(1 * GB, application_1);

    // task_1_0 (1G) application_0 moved to b2 with max running app 1 so it is
    // not scheduled
    checkNodeResourceUsage(4 * GB, nm_0);
    checkNodeResourceUsage(0 * GB, nm_1);

  }

  private int getNumAppsInQueue(String name, List<CSQueue> queues) {
    for (CSQueue queue : queues) {
      if (queue.getQueueShortName().equals(name)) {
        return queue.getNumApplications();
      }
    }
    return -1;
  }

  private CapacitySchedulerQueueInfo getQueueInfo(String name,
                                                  CapacitySchedulerQueueInfoList info) {
    if (info != null) {
      for (CapacitySchedulerQueueInfo queueInfo : info.getQueueInfoList()) {
        if (queueInfo.getQueueName().equals(name)) {
          return queueInfo;
        } else {
          CapacitySchedulerQueueInfo result =
              getQueueInfo(name, queueInfo.getQueues());
          if (result == null) {
            continue;
          }
          return result;
        }
      }
    }
    return null;
  }


  private void verifyQueueMetrics(QueueMetrics queue) {
    Assert.assertEquals(0, queue.getPendingMB());
    Assert.assertEquals(0, queue.getActiveUsers());
    Assert.assertEquals(0, queue.getActiveApps());
    Assert.assertEquals(0, queue.getAppsPending());
    Assert.assertEquals(0, queue.getAppsRunning());
    Assert.assertEquals(0, queue.getAllocatedMB());
    Assert.assertEquals(0, queue.getAllocatedVirtualCores());
  }

  private Configuration getCapacityConfiguration(Configuration config) {
    CapacitySchedulerConfiguration conf =
        new CapacitySchedulerConfiguration(config);

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{"a", "b"});
    conf.setCapacity(A, 50);
    conf.setCapacity(B, 50);
    conf.setQueues(A, new String[]{"a1", "a2"});
    conf.setCapacity(A1, 50);
    conf.setCapacity(A2, 50);
    conf.setQueues(B, new String[]{"b1"});
    conf.setCapacity(B1, 100);
    return conf;
  }

}
