/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.orca.clouddriver.tasks;

import com.netflix.spinnaker.orca.api.pipeline.RetryableTask;
import com.netflix.spinnaker.orca.api.pipeline.TaskResult;
import com.netflix.spinnaker.orca.api.pipeline.models.StageExecution;
import java.time.Duration;
import javax.annotation.Nonnull;
import org.springframework.stereotype.Component;

@Component
public class MonitorCloudOperationTask implements RetryableTask {

  @Nonnull
  @Override
  public TaskResult execute(@Nonnull StageExecution stage) {
    // TODO(rz): This would basically be the same as MonitorKatoTask.
    return TaskResult.SUCCEEDED;
  }

  @Override
  public Duration getBackoffPeriod() {
    return Duration.ofSeconds(5);
  }

  @Override
  public Duration getTimeout() {
    return Duration.ofHours(1);
  }
}
