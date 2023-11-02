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

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.protocolrecords.SetApplicationTagsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SetApplicationTagsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SetApplicationTagsResponseProtoOrBuilder;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.APPLICATION_TAG_FORCE_LOWERCASE_CONVERSION;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_APPLICATION_TAG_FORCE_LOWERCASE_CONVERSION;

@Private
@Unstable
public class SetApplicationTagsResponsePBImpl extends SetApplicationTagsResponse {
  private static volatile Boolean forceLowerCaseTags;
  SetApplicationTagsResponseProto proto = SetApplicationTagsResponseProto.getDefaultInstance();
  SetApplicationTagsResponseProto.Builder builder = null;
  boolean viaProto = false;

  private Set<String> applicationTags = null;

  public SetApplicationTagsResponsePBImpl() {
    builder = SetApplicationTagsResponseProto.newBuilder();
    initLowerCaseConfig();
  }

  public SetApplicationTagsResponsePBImpl(SetApplicationTagsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
    initLowerCaseConfig();
  }

  private static void initLowerCaseConfig() {
    if (forceLowerCaseTags == null) { // TODO: performance :(
      Configuration conf = new Configuration();

      forceLowerCaseTags =
          conf.getBoolean(APPLICATION_TAG_FORCE_LOWERCASE_CONVERSION,
              DEFAULT_APPLICATION_TAG_FORCE_LOWERCASE_CONVERSION);
    }
  }

  public SetApplicationTagsResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void mergeLocalToBuilder() {
    if (applicationTags != null && !applicationTags.isEmpty()) {
      builder.clearApplicationTags();
      builder.addAllApplicationTags(this.applicationTags);
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SetApplicationTagsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void initApplicationTags() {
    if (this.applicationTags != null) {
      return;
    }
    SetApplicationTagsResponseProtoOrBuilder p = viaProto ? proto : builder;
    this.applicationTags = new HashSet<>();
    this.applicationTags.addAll(p.getApplicationTagsList());
  }

  @Override
  public Set<String> getApplicationTags() {
    initApplicationTags();
    return this.applicationTags;
  }

  @Override
  public void setApplicationTags(Set<String> tags) {
    maybeInitBuilder();
    if (tags == null || tags.isEmpty()) {
      builder.clearApplicationTags();
      this.applicationTags = null;
      return;
    }
    // Convert applicationTags to lower case and add
    this.applicationTags = new HashSet<>();
    for (String tag : tags) {
      this.applicationTags.add(
          forceLowerCaseTags ? StringUtils.toLowerCase(tag) : tag);
    }
  }
}
