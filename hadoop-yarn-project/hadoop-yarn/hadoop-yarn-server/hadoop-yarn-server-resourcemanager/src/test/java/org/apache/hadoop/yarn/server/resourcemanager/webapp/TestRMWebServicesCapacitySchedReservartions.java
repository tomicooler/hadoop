package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import com.sun.jersey.api.client.ClientResponse;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationConstants;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlanQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ReservationQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.junit.Test;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfigGeneratorForTest.createConfiguration;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerTestUtilities.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.assertJsonResponse;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createRM;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.TestWebServiceUtil.createWebAppDescriptor;

public class TestRMWebServicesCapacitySchedReservartions extends
    JerseyTestBase {

  public TestRMWebServicesCapacitySchedReservartions() {
    super(createWebAppDescriptor());
  }

  @Test
  public void testSchedulerResponseReservations()
      throws Exception {
    Map<String, String> conf = new HashMap<>();
    conf.put("yarn.scheduler.capacity.legacy-queue-mode.enabled", "true");
    conf.put("yarn.scheduler.capacity.root.queues", "default, parent");
    conf.put("yarn.scheduler.capacity.root.default.capacity", "25");
    conf.put("yarn.scheduler.capacity.root.parent.capacity", "75");
    conf.put("yarn.scheduler.capacity.root.parent.reservable", "true");
    conf.put("yarn.scheduler.capacity.root.parent.reservation-window", "true");
    conf.put("yarn.scheduler.capacity.root.parent.average-capacity", "15");


    try (MockRM rm = createRM(createConfiguration(conf))) {
      rm.registerNode("h1:1234", 32 * GB, 32);
      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

      ((ReservationQueue) cs
          .getQueue("parent" + ReservationConstants.DEFAULT_QUEUE_SUFFIX))
          .setEntitlement(
              new QueueEntitlement(0f, 1f));

      ReservationQueue a =
          new ReservationQueue(cs.getQueueContext(), "a", (PlanQueue) cs.getQueue("parent"));
      cs.addQueue(a);
      a.setEntitlement(new QueueEntitlement(0.25f, 1f));

      ReservationQueue b =
          new ReservationQueue(cs.getQueueContext(), "b", (PlanQueue) cs.getQueue("parent"));
      cs.addQueue(b);
      cs.setEntitlement("b", new QueueEntitlement(0.75f, 1.0f));

      assertJsonResponse(sendRequest(),
          "webapp/scheduler-response-Reservations.json");

      //cs.reinitialize(rm.getConfig(), rm.getRMContext());

      //assertJsonResponse(sendRequest(),
      //    "webapp/scheduler-response-Reservations.json");
    }
  }

  private ClientResponse sendRequest() {
    return resource().path("ws").path("v1").path("cluster")
        .path("scheduler").accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
  }
}
