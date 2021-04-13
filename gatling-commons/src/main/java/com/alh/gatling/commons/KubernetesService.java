package com.walmart.gatling.commons;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.ExtensionsV1beta1Api;
import io.kubernetes.client.models.ExtensionsV1beta1Deployment;
import io.kubernetes.client.models.ExtensionsV1beta1DeploymentCondition;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1DeploymentCondition;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Config;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.reflect.TypeToken;
import com.squareup.okhttp.Call;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;

import javax.inject.Singleton;


@Singleton
public class KubernetesService {
  private static final Logger log = LoggerFactory.getLogger(KubernetesService.class);

  private ApiClient apiClient;
  private AppsV1Api appsApi;
  private CoreV1Api coreApi;


  private ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

  public KubernetesService(){


    try {
      this.apiClient = Config.defaultClient();
      Configuration.setDefaultApiClient(apiClient);
    }catch (IOException e){
      log.warn("Couldn't generate client for kubernetes");
      e.printStackTrace();
    }


    this.appsApi = new AppsV1Api(apiClient);

    this.coreApi = new CoreV1Api(apiClient);
  }

  private static String deploymentNameFor(String testExecutionId){
    return String.format("gatling-worker.%s", testExecutionId);
  }

  private V1Deployment readDeploymentFile(){
    V1Deployment deployment;
    try {
      deployment = mapper
          .readerFor(V1Deployment.class)
          .readValue(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream("worker-pod.yaml" )));
    } catch(IOException e){
      log.warn("Can't read the yaml file");
      e.printStackTrace();
      return null;
    }
    return deployment;
  }

  public String createDeploy(String testExecutionId){
    V1Deployment deployment = readDeploymentFile();

    deployment.getSpec().setReplicas(1);
    deployment.getMetadata().setName(deploymentNameFor(testExecutionId));
    deployment.getMetadata().setNamespace("default");

    V1Deployment newDeployment;
    try{
      newDeployment = appsApi.createNamespacedDeployment("default", deployment, null, null,null);
    } catch (ApiException e){
      log.warn("Cand create deployment for {}", testExecutionId );
      return null;
    }

    return newDeployment.getMetadata().getName();

  }


  private Watch<V1Deployment> createDeploymentWatch(String name) throws ApiException {
    Call call = appsApi.listNamespacedDeploymentCall(
        "default", null, null, null, null,
        null, null, null, null,
        true, null, null);
    Type watchType = new TypeToken<Watch.Response<V1Deployment>>() {
    }.getType();

    return Watch.createWatch(apiClient, call, watchType);
  }

  private boolean replicasReady(V1Deployment deployment, Integer expectedReplicas, String expectedName) {
    String deploymentName = deployment.getMetadata().getName();
    Integer readyReplicas = deployment.getStatus().getReadyReplicas();
    log.info("Deployment {} - {}/{} replicas are ready", deploymentName,
             readyReplicas, expectedReplicas);
    if (!expectedName.equals(deploymentName)) {
      log.info("Deployment {} - watch event has different deployment name {}", expectedName, deploymentName);
      return false;
    }
    List<V1DeploymentCondition> conditionList = deployment.getStatus().getConditions();
    if (conditionList == null) {
      log.warn("Deployment {} - watch event has null conditions null", deploymentName);
      return false;
    }
    boolean readyReplicasCondition = readyReplicas != null && readyReplicas.equals(expectedReplicas);
    boolean availableCondition = conditionList.stream()
        .anyMatch(condition -> "Available".equals(condition.getType()) && "True".equals(condition.getStatus()));
    if (availableCondition && readyReplicasCondition) {
      log.info("Deployment {} - {}/{} has all replicas ready", deploymentName,
               readyReplicas, expectedReplicas);
      return true;
    }
    return false;
  }

  public void waitUntilDeploymentIsReady(String name, int expectedReplicas) {
    try (Watch<V1Deployment> watch = createDeploymentWatch(name);) {
      for (Watch.Response<V1Deployment> item : watch) {
        V1Deployment deployment = item.object;
        if (deployment != null && deployment.getMetadata() != null && deployment.getStatus() != null) {
          if (replicasReady(deployment, expectedReplicas, name)) {
            break;
          }
        } else {
          log.warn("Null watch event for deployment {}, {}", name, deployment);
        }
      }
    } catch (ApiException | IOException e) {
      log.warn("Failed to wath after deployment {}", name);
    }
  }

  public void deleteDeployment(String testExecutionId) {
    try  {
      String deploymentName = deploymentNameFor(testExecutionId);
      appsApi.deleteNamespacedDeployment(deploymentName, "default", new V1DeleteOptions(), "true", null, null, null,null);
    } catch (ApiException e) {
      log.warn("Failed to call AppsV1Api#createNamespacedDeployment");
    }
  }
}


