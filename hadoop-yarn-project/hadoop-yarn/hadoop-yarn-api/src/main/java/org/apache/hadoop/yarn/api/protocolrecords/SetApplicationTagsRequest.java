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

package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The request sent by the client to the <code>ResourceManager</code>
 * to set the application tags.</p>
 *
 * <p>The request includes the {@link ApplicationId} of the application and
 * the application tags to set.</p>
 *
 * @see ApplicationClientProtocol#setApplicationTags(SetApplicationTagsRequest)
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class SetApplicationTagsRequest {

  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public static SetApplicationTagsRequest newInstance(ApplicationId applicationId,
                                                      Set<String> applicationTags) {
    SetApplicationTagsRequest request =
        Records.newRecord(SetApplicationTagsRequest.class);
    request.setApplicationId(applicationId);
    request.setApplicationTags(applicationTags);
    return request;
  }

  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public abstract ApplicationId getApplicationId();

  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public abstract void setApplicationId(ApplicationId applicationId);

  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public abstract Set<String> getApplicationTags();

  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public abstract void setApplicationTags(Set<String> applicationTags);
}
