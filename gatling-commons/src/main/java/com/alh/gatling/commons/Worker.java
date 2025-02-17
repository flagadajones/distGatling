/*
 *
 *   Copyright 2016 alh Technology
 *  
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package com.alh.gatling.commons;

import akka.actor.AbstractActor;
import akka.actor.ActorInitializationException;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.DeathPactException;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.ReceiveTimeout;
import akka.actor.SupervisorStrategy;
import akka.actor.Terminated;
import akka.cluster.client.ClusterClient;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Procedure;
import com.alh.gatling.commons.Master.Ack;
import com.alh.gatling.commons.Master.Job;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.UUID;

import static akka.actor.SupervisorStrategy.*;

public class Worker extends AbstractActor {

    private final ActorRef clusterClient;
    private final String host;
    private String workerRole;
    private final String workerId;
    private final ActorRef workExecutor;
    private final Cancellable registerTask;
    private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    private String currentJobId = null;
    private final Cancellable keepAliveTask;

    private final Procedure<Object> working = new Procedure<Object>() {
        public void apply(Object message) {
            //log.info("Work received. Result {}.", message);
            if (message instanceof WorkComplete) {
                Object result = ((WorkComplete) message).result;
                //log.info("Work is complete. Result {}.", result);
                sendToMaster(new MasterWorkerProtocol.WorkIsDone(workerId, jobId(), result));
                getContext().setReceiveTimeout(Duration.create(5, "seconds"));
                Procedure<Object> waitForWorkIsDoneAck = waitForWorkIsDoneAck(result);
                getContext().become(receiveBuilder()
                                        .matchAny(p -> waitForWorkIsDoneAck.apply(p))
                                        .build());
            }
            else if (message instanceof  Worker.FileUploadComplete){
                sendToMaster(message);
                getContext().become(receiveBuilder()
                                        .matchAny(p->idle.apply(p))
                                        .build());
            }
            else if (message instanceof WorkFailed) {
                Object result = ((WorkFailed) message).result;
                log.info("Work is failed. Result {}.", result);
                sendToMaster(new MasterWorkerProtocol.WorkFailed(workerId, jobId(),result));
                getContext().become(receiveBuilder()
                                        .matchAny(p->idle.apply(p))
                                        .build());
                //getContext().setReceiveTimeout(Duration.create(5, "seconds"));
                ///Procedure<Object> waitForWorkIsDoneAck = waitForWorkIsDoneAck(result);
                //getContext().become(waitForWorkIsDoneAck);
            }
            else if(message==KeepAliveTick){
                log.info("Job is in progress. {}.", jobId());
                if (currentJobId!=null){
                    sendToMaster(new MasterWorkerProtocol.WorkInProgress(workerId, jobId()));
                }
            }else if (message instanceof Job) {
                log.info("Yikes. Master told me to do work, while I'm working.");
            } else {
                unhandled(message);
            }
        }
    };

    private final Procedure<Object> idle = new Procedure<Object>() {
        public void apply(Object message) {
            if(message==KeepAliveTick){
                sendToMaster(new MasterWorkerProtocol.WorkerRequestsFile(workerId, workerRole, host));
            }
            else if (message instanceof MasterWorkerProtocol.WorkIsReady)
                sendToMaster(new MasterWorkerProtocol.WorkerRequestsWork(workerId, workerRole));
            else if (message instanceof Master.Job) {
                Job job = (Job) message;
                log.info("Got work: {}", job);
                currentJobId = job.jobId;
                workExecutor.tell(job, getSelf());
                getContext().become(receiveBuilder()
                                        .matchAny(p->working.apply(p))
                                        .build());
            }
            else if (message instanceof Master.FileJob) {
                Master.FileJob fileJob = (Master.FileJob) message;
                log.info("Got file upload work: {}", fileJob.uploadFileRequest);
                currentJobId = fileJob.jobId;
                workExecutor.tell(fileJob, getSelf());
                getContext().become(receiveBuilder()
                                        .matchAny(p->working.apply(p))
                                        .build());
            }
            else
                unhandled(message);
        }
    };

    public static final Object KeepAliveTick = new Object() {
        @Override
        public String toString() {
            return "KeepAliveTick";
        }
    };

    public Worker(ActorRef clusterClient, Props workExecutorProps, FiniteDuration registerInterval, String workerRole) {
        this.clusterClient = clusterClient;
        this.workerRole = workerRole;
        if (System.getenv("HOSTNAME") == null || !System.getenv("HOSTNAME").contains("-")){
            this.workerId = UUID.randomUUID().toString();
        }else{
            this.workerId =  System.getenv("HOSTNAME").substring(0,System.getenv("HOSTNAME").lastIndexOf("-"));
        }
        this.host = HostUtils.lookupIp();
        this.workExecutor = getContext().watch(getContext().actorOf(workExecutorProps, "exec"));
        this.registerTask = getContext().system().scheduler().schedule
            (
                Duration.Zero(),
                registerInterval,
                clusterClient,
                new ClusterClient.SendToAll("/user/master/singleton", new MasterWorkerProtocol.RegisterWorker(workerId)),
                getContext().dispatcher(),
                getSelf()
            );
        FiniteDuration workTimeout = Duration.create(60, "seconds");
        this.keepAliveTask = getContext().system().scheduler().schedule(workTimeout.div(2), workTimeout.div(2), getSelf(), KeepAliveTick, getContext().dispatcher(), getSelf());
    }

    public static Props props(ActorRef clusterClient, Props workExecutorProps, FiniteDuration registerInterval,String workerRole) {
        return Props.create(Worker.class, clusterClient, workExecutorProps, registerInterval, workerRole);
    }

    public static Props props(ActorRef clusterClient, Props workExecutorProps,String workerRole) {
        return props(clusterClient, workExecutorProps, Duration.create(10, "seconds"),workerRole);
    }

    private String jobId() {
        if (currentJobId!=null)
            return currentJobId;
        else
            throw new IllegalStateException("Not working");
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return new OneForOneStrategy(-1, Duration.Inf(),
                                     t -> {
                                         log.info("Throwable, Work is failed for1 "+ t);
                                         if (t instanceof ActorInitializationException)
                                             return stop();
                                         else if (t instanceof DeathPactException)
                                             return stop();
                                         else if (t instanceof RuntimeException) {
                                             if (currentJobId!=null) {
                                                 log.info("RuntimeException, Work is failed for "+ currentJobId);
                                                 sendToMaster(new MasterWorkerProtocol.WorkFailed(workerId, jobId(),new Result(-1,"","","",null)));
                                             }
                                             getContext().become(receiveBuilder()
                                                                     .matchAny(p->idle.apply(p))
                                                                     .build());
                                             return restart();
                                         }
                                         else if (t instanceof Exception) {
                                             if (currentJobId!=null) {
                                                 log.info("Exception, Work is failed for "+ currentJobId);
                                                 sendToMaster(new MasterWorkerProtocol.WorkFailed(workerId, jobId(),new Result(-1,"","","",null)));
                                             }
                                             getContext().become(receiveBuilder()
                                                                     .matchAny(p->idle.apply(p))
                                                                     .build());
                                             return restart();
                                         }
                                         else {
                                             log.info("Throwable, Work is failed for "+ t);
                                             return escalate();
                                         }
                                     }
        );
    }

    @Override
    public void postStop() {
        registerTask.cancel();
        keepAliveTask.cancel();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .build();
    }

    public void onReceive(Object message) {
        unhandled(message);
    }

    private Procedure<Object> waitForWorkIsDoneAck(final Object result) {
        return message -> {
            if (message instanceof Ack && ((Ack) message).workId.equals(jobId())) {
                sendToMaster(new MasterWorkerProtocol.WorkerRequestsWork(workerId, workerRole));
                getContext().setReceiveTimeout(Duration.Undefined());
                getContext().become(receiveBuilder()
                                        .matchAny(p->idle.apply(p))
                                        .build());

            } else if (message instanceof ReceiveTimeout) {
                log.info("No ack from master, retrying (" + workerId + " -> " + jobId() + ")");
                sendToMaster(new MasterWorkerProtocol.WorkIsDone(workerId, jobId(), result));
            } else if (message instanceof Master.AckKubernetes && ((Master.AckKubernetes) message).workId.equals(jobId())){
                getContext().setReceiveTimeout(Duration.Undefined());
                log.info("Workerul receive ack for work done in kubernetes");
            } else {
                unhandled(message);
            }
        };
    }

    {
        getContext().become(receiveBuilder()
                                .matchAny(p->idle.apply(p))
                                .build());
    }

    @Override
    public void unhandled(Object message) {
        if(message==KeepAliveTick){
            //do nothing
        }
        else if (message instanceof Terminated && ((Terminated) message).getActor().equals(workExecutor)) {
            log.info("Received Terminated from exec.");
            getContext().stop(getSelf());
        } else if (message instanceof MasterWorkerProtocol.WorkIsReady) {
            // do nothing
        } else {
            super.unhandled(message);
        }
    }

    private void sendToMaster(Object msg) {
        clusterClient.tell(new ClusterClient.SendToAll("/user/master/singleton", msg), getSelf());
    }

    public static final class WorkComplete implements Serializable {
        public final Object result;

        public WorkComplete(Object result) {
            this.result = result;
        }

        @Override
        public String toString() {
            return "WorkComplete{" +
                   "result=" + result +
                   '}';
        }
    }

    public static final class FileUploadComplete implements Serializable {
        public final Master.UploadFile result;
        public final String host;

        public FileUploadComplete(Master.UploadFile result, String host) {
            this.result = result;
            this.host = host;
        }

        @Override
        public String toString() {
            return "FileUploadComplete{" +
                   "result=" + result +
                   ", host='" + host + '\'' +
                   '}';
        }
    }

    public static final class WorkFailed implements Serializable {
        public final Object result;

        public WorkFailed(Object result) {
            this.result = result;
        }

        @Override
        public String toString() {
            return "WorkFailed{" +
                   "result=" + result +
                   '}';
        }
    }

    public static final class Result implements Serializable {
        public final int result;
        public final String errPath;
        public final String stdPath;
        public final String metrics;
        public final Job job;

        public Result(int result, String errPath, String stdPath, String metrics, Job job) {
            this.result = result;
            this.errPath = errPath;
            this.stdPath = stdPath;
            this.metrics = metrics;
            this.job = job;
        }


        @Override
        public String toString() {
            return "Result{" +
                   "result=" + result +
                   ", errPath='" + errPath + '\'' +
                   ", stdPath='" + stdPath + '\'' +
                   ", metrics='" + metrics + '\'' +
                   ", job=" + job +
                   '}';
        }
    }


}
