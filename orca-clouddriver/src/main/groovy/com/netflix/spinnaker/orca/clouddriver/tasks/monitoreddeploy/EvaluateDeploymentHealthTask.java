/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.spinnaker.orca.clouddriver.tasks.monitoreddeploy;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spinnaker.config.DeploymentMonitorDefinition;
import com.netflix.spinnaker.config.DeploymentMonitorServiceProvider;
import com.netflix.spinnaker.orca.ExecutionStatus;
import com.netflix.spinnaker.orca.TaskResult;
import com.netflix.spinnaker.orca.clouddriver.pipeline.servergroup.strategies.MonitoredDeployStageData;
import com.netflix.spinnaker.orca.deploymentmonitor.models.EvaluateHealthRequest;
import com.netflix.spinnaker.orca.deploymentmonitor.models.EvaluateHealthResponse;
import com.netflix.spinnaker.orca.pipeline.model.Stage;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(value = "monitored-deploy.enabled")
public class EvaluateDeploymentHealthTask extends MonitoredDeployBaseTask {
  @Autowired
  EvaluateDeploymentHealthTask(
      DeploymentMonitorServiceProvider deploymentMonitorServiceProvider, Registry registry) {
    super(deploymentMonitorServiceProvider, registry);
  }

  @Override
  public @Nonnull TaskResult executeInternal(
      Stage stage,
      MonitoredDeployStageData context,
      DeploymentMonitorDefinition monitorDefinition) {

    // skipToPercentage can be either in "outputs" of a prior stage in which case we should get it
    // via our context
    Integer skipToPercentage = (Integer) stage.getContext().getOrDefault("skipToPercentage", 0);
    Stage createServerGroupStage = stage.getParent();

    if ((skipToPercentage == 0) && (createServerGroupStage != null)) {
      // If not... then it will be set by deck's call to PATCH on the createServerGroup stage
      skipToPercentage =
          (int) createServerGroupStage.getContext().getOrDefault("skipToPercentage", 0);
    }

    EvaluateHealthRequest request = new EvaluateHealthRequest(stage);
    log.info(
        "Evaluating health of deployment {} at {}% with {}",
        request.getNewServerGroup(), request.getCurrentProgress(), monitorDefinition);

    if (skipToPercentage > request.getCurrentProgress()) {
      String skipMessage =
          String.format(
              "Skipping remaining health evaluation because the user requested to skip to %d percent",
              skipToPercentage);
      log.warn(skipMessage);

      return buildTaskResult(TaskResult.builder(ExecutionStatus.SUCCEEDED), skipMessage);
    }

    EvaluateHealthResponse response = monitorDefinition.getService().evaluateHealth(request);
    sanitizeAndLogResponse(response, monitorDefinition, stage.getExecution().getId());

    EvaluateHealthResponse.NextStepDirective directive = response.getNextStep().getDirective();

    Id statusCounterId =
        registry
            .createId("deploymentMonitor.healthStatus")
            .withTag("monitorId", monitorDefinition.getId())
            .withTag("status", directive.toString());

    registry.counter(statusCounterId).increment();

    if (directive != EvaluateHealthResponse.NextStepDirective.WAIT) {
      // We want to log the time it takes for the monitor to actually produce a decision
      // since wait is a not a decision - no need to time it
      Id timerId =
          registry
              .createId("deploymentMonitor.timing")
              .withTag("monitorId", monitorDefinition.getId())
              .withTag("status", directive.toString());

      long duration = Instant.now().toEpochMilli() - stage.getStartTime();
      registry.timer(timerId).record(duration, TimeUnit.MILLISECONDS);
    }

    return buildTaskResult(processDirective(directive, monitorDefinition), response);
  }

  private TaskResult.TaskResultBuilder processDirective(
      EvaluateHealthResponse.NextStepDirective directive,
      DeploymentMonitorDefinition monitorDefinition) {
    switch (directive) {
      case COMPLETE:
        return TaskResult.builder(ExecutionStatus.SUCCEEDED).output("skipToPercentage", 100);

      case CONTINUE:
        return TaskResult.builder(ExecutionStatus.SUCCEEDED);

      case WAIT:
        return TaskResult.builder(ExecutionStatus.RUNNING);

      case ABORT:
        return TaskResult.builder(ExecutionStatus.TERMINAL);

      default:
        throw new DeploymentMonitorInvalidDataException(
            String.format(
                "Invalid next step directive: %s received from Deployment Monitor: %s",
                directive, monitorDefinition));
    }
  }
}
