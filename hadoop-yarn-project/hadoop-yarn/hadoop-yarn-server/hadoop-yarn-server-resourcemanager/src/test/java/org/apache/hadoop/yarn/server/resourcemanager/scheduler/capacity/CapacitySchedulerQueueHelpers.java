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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.List;

import static org.junit.Assert.assertEquals;

public final class CapacitySchedulerQueueHelpers {

  public static final String A = CapacitySchedulerConfiguration.ROOT + ".a";
  public static final String B = CapacitySchedulerConfiguration.ROOT + ".b";
  public static final String A1 = A + ".a1";
  public static final String A2 = A + ".a2";
  public static final String B1 = B + ".b1";
  public static final String B2 = B + ".b2";
  public static final String B3 = B + ".b3";
  public static final float A_CAPACITY = 10.5f;
  public static final float B_CAPACITY = 89.5f;
  public static final String P1 = CapacitySchedulerConfiguration.ROOT + ".p1";
  public static final String P2 = CapacitySchedulerConfiguration.ROOT + ".p2";
  public static final String X1 = P1 + ".x1";
  public static final String X2 = P1 + ".x2";
  public static final String Y1 = P2 + ".y1";
  public static final String Y2 = P2 + ".y2";
  public static final float A1_CAPACITY = 30;
  public static final float A2_CAPACITY = 70;
  public static final float B1_CAPACITY = 79.2f;
  public static final float B2_CAPACITY = 0.8f;
  public static float B3_CAPACITY = 20;

  private CapacitySchedulerQueueHelpers() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * @param conf, to be modified
   * @return
   *           root
   *          /      \
   *        a         b
   *       / \     /  |  \
   *      a1  a2  b1  b2 b3
   *
   */
  public static CapacitySchedulerConfiguration setupQueueConfiguration(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[]{"a", "b"});

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[]{"a1", "a2"});
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    conf.setQueues(B, new String[]{"b1", "b2", "b3"});
    conf.setCapacity(B1, B1_CAPACITY);
    conf.setUserLimitFactor(B1, 100.0f);
    conf.setCapacity(B2, B2_CAPACITY);
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, B3_CAPACITY);
    conf.setUserLimitFactor(B3, 100.0f);

    return conf;
  }

  /**
   * @param conf, to be modified
   * @return CS configuration which has deleted all childred of queue(b)
   *           root
   *          /     \
   *        a        b
   *       / \
   *      a1  a2
   */
  public static CapacitySchedulerConfiguration setupQueueConfWithOutChildrenOfB(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{"a", "b"});

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[]{"a1", "a2"});
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    return conf;
  }

  /**
   * @param conf, to be modified
   * @return CS configuration which has deleted a queue(b1)
   *           root
   *          /     \
   *        a        b
   *       / \       | \
   *      a1  a2    b2  b3
   */
  public static CapacitySchedulerConfiguration setupQueueConfigurationWithOutB1(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{"a", "b"});

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[]{"a1", "a2"});
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    conf.setQueues(B, new String[]{"b2", "b3"});
    conf.setCapacity(B2, B2_CAPACITY + B1_CAPACITY); //as B1 is deleted
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, B3_CAPACITY);
    conf.setUserLimitFactor(B3, 100.0f);

    return conf;
  }

  /**
   * @param conf, to be modified
   * @return CS configuration which has converted b1 to parent queue
   *           root
   *          /     \
   *        a        b
   *       / \    /  |  \
   *      a1  a2 b1  b2  b3
   *              |
   *             b11
   */
  public static CapacitySchedulerConfiguration setupQueueConfigurationWithB1AsParentQueue(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{"a", "b"});

    conf.setCapacity(A, A_CAPACITY);
    conf.setCapacity(B, B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[]{"a1", "a2"});
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    conf.setQueues(B, new String[]{"b1", "b2", "b3"});
    conf.setCapacity(B1, B1_CAPACITY);
    conf.setUserLimitFactor(B1, 100.0f);
    conf.setCapacity(B2, B2_CAPACITY);
    conf.setUserLimitFactor(B2, 100.0f);
    conf.setCapacity(B3, B3_CAPACITY);
    conf.setUserLimitFactor(B3, 100.0f);

    // Set childQueue for B1
    conf.setQueues(B1, new String[]{"b11"});
    String B11 = B1 + ".b11";
    conf.setCapacity(B11, 100.0f);
    conf.setUserLimitFactor(B11, 100.0f);

    return conf;
  }

  /**
   * @param conf, to be modified
   * @return CS configuration which has deleted a Parent queue(b)
   */
  public static CapacitySchedulerConfiguration setupQueueConfigurationWithOutB(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[]{"a"});

    conf.setCapacity(A, A_CAPACITY + B_CAPACITY);

    // Define 2nd-level queues
    conf.setQueues(A, new String[]{"a1", "a2"});
    conf.setCapacity(A1, A1_CAPACITY);
    conf.setUserLimitFactor(A1, 100.0f);
    conf.setCapacity(A2, A2_CAPACITY);
    conf.setUserLimitFactor(A2, 100.0f);

    return conf;
  }

  public static CapacitySchedulerConfiguration setupBlockedQueueConfiguration(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{"a", "b"});

    conf.setCapacity(A, 80f);
    conf.setCapacity(B, 20f);
    conf.setUserLimitFactor(A, 100);
    conf.setUserLimitFactor(B, 100);
    conf.setMaximumCapacity(A, 100);
    conf.setMaximumCapacity(B, 100);
    return conf;
  }

  public static CapacitySchedulerConfiguration setupOtherBlockedQueueConfiguration(
      CapacitySchedulerConfiguration conf) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[]{"p1", "p2"});

    conf.setCapacity(P1, 50f);
    conf.setMaximumCapacity(P1, 50f);
    conf.setCapacity(P2, 50f);
    conf.setMaximumCapacity(P2, 100f);
    // Define 2nd-level queues
    conf.setQueues(P1, new String[]{"x1", "x2"});
    conf.setCapacity(X1, 80f);
    conf.setMaximumCapacity(X1, 100f);
    conf.setUserLimitFactor(X1, 2f);
    conf.setCapacity(X2, 20f);
    conf.setMaximumCapacity(X2, 100f);
    conf.setUserLimitFactor(X2, 2f);

    conf.setQueues(P2, new String[]{"y1", "y2"});
    conf.setCapacity(Y1, 80f);
    conf.setUserLimitFactor(Y1, 2f);
    conf.setCapacity(Y2, 20f);
    conf.setUserLimitFactor(Y2, 2f);
    return conf;
  }

  public static void checkQueueCapacities(CapacityScheduler cs,
                                          float capacityA, float capacityB) {
    CSQueue rootQueue = cs.getRootQueue();
    CSQueue queueA = findQueue(rootQueue, A);
    CSQueue queueB = findQueue(rootQueue, B);
    CSQueue queueA1 = findQueue(queueA, A1);
    CSQueue queueA2 = findQueue(queueA, A2);
    CSQueue queueB1 = findQueue(queueB, B1);
    CSQueue queueB2 = findQueue(queueB, B2);
    CSQueue queueB3 = findQueue(queueB, B3);

    float capA = capacityA / 100.0f;
    float capB = capacityB / 100.0f;

    checkQueueCapacity(queueA, capA, capA, 1.0f, 1.0f);
    checkQueueCapacity(queueB, capB, capB, 1.0f, 1.0f);
    checkQueueCapacity(queueA1, A1_CAPACITY / 100.0f,
        (A1_CAPACITY / 100.0f) * capA, 1.0f, 1.0f);
    checkQueueCapacity(queueA2, A2_CAPACITY / 100.0f,
        (A2_CAPACITY / 100.0f) * capA, 1.0f, 1.0f);
    checkQueueCapacity(queueB1, B1_CAPACITY / 100.0f,
        (B1_CAPACITY / 100.0f) * capB, 1.0f, 1.0f);
    checkQueueCapacity(queueB2, B2_CAPACITY / 100.0f,
        (B2_CAPACITY / 100.0f) * capB, 1.0f, 1.0f);
    checkQueueCapacity(queueB3, B3_CAPACITY / 100.0f,
        (B3_CAPACITY / 100.0f) * capB, 1.0f, 1.0f);
  }

  public static void checkQueueCapacity(CSQueue q, float expectedCapacity,
                                        float expectedAbsCapacity, float expectedMaxCapacity,
                                        float expectedAbsMaxCapacity) {
    final float epsilon = 1e-5f;
    assertEquals("capacity", expectedCapacity, q.getCapacity(), epsilon);
    assertEquals("absolute capacity", expectedAbsCapacity,
        q.getAbsoluteCapacity(), epsilon);
    assertEquals("maximum capacity", expectedMaxCapacity,
        q.getMaximumCapacity(), epsilon);
    assertEquals("absolute maximum capacity", expectedAbsMaxCapacity,
        q.getAbsoluteMaximumCapacity(), epsilon);
  }

  public static CSQueue findQueue(CSQueue root, String queuePath) {
    if (root.getQueuePath().equals(queuePath)) {
      return root;
    }

    List<CSQueue> childQueues = root.getChildQueues();
    if (childQueues != null) {
      for (CSQueue q : childQueues) {
        if (queuePath.startsWith(q.getQueuePath())) {
          CSQueue result = findQueue(q, queuePath);
          if (result != null) {
            return result;
          }
        }
      }
    }

    return null;
  }

}
