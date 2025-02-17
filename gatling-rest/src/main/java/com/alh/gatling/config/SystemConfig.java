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

package com.alh.gatling.config;

import com.alh.gatling.init.ClusterFactory;
import com.alh.gatling.commons.AgentConfig;
import com.alh.gatling.commons.HostUtils;
import com.alh.gatling.commons.MasterClientActor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.routing.RoundRobinPool;

/**
 * . A spring configuration object to create beans
 */
@Configuration
public class SystemConfig {

    @Value("${actor.numberOfActors}")
    private int numberOfActors;

    @Value("${actor.port}")
    private int port;

    @Value("${actor.role}")
    private String role;

    @Value("${actor.executerType}")
    private String executerType;

    @Value("${server.port}")
    private int clientPort;


    /**
     * bean factory to create the agent configuration
     * @param env
     * @return
     */
    @Bean
    public AgentConfig configBuilder(Environment env){
        AgentConfig agentConfig = new AgentConfig();

        AgentConfig.Actor actor = new AgentConfig.Actor();
        actor.setExecuterType(executerType);
        actor.setNumberOfActors(numberOfActors);
        actor.setPort(port);
        actor.setRole(role);
        agentConfig.setActor(actor);

        AgentConfig.Job jobInfo = new AgentConfig.Job();
        jobInfo.setArtifact(env.getProperty("job.artifact"));
        jobInfo.setCommand(env.getProperty("job.command"));
        jobInfo.setPath(env.getProperty("job.path"));
        jobInfo.setLogDirectory(env.getProperty("job.logDirectory"));
        jobInfo.setExitValues(new int[]{0,2});
        agentConfig.setJob(jobInfo);

        AgentConfig.LogServer logServer = new AgentConfig.LogServer();
        logServer.setHostName(HostUtils.lookupIp());
        logServer.setPort(clientPort);
        agentConfig.setLogServer(logServer);

        return agentConfig;
    }

    /**
     * bean factory to create the actor system and creating the master actor
     * the master actor is a singleton with a persistent store
     * @param agentConfig
     * @param port
     * @param masterName
     * @param isPrimary
     * @return
     */
    @Bean
    public ActorSystem createActorSystemWithMaster(AgentConfig agentConfig,
                                                   @Value("${master.port}") int port,
                                                   @Value("${master.name}") String masterName,
                                                   @Value("${master.primary}") boolean isPrimary,
                                                   @Value("${master.kubernetes}") boolean isRunningOnKubernetes) {

        return ClusterFactory.startMaster(port,masterName,isPrimary,agentConfig,isRunningOnKubernetes);
    }


    /**
     * bean factory to create pool of the master client actors, the pool is used in a round robin manner
     * @param system
     * @param pool
     * @param masterName
     * @return
     */
    @Bean
    public ActorRef createRouter(ActorSystem system,
                                 @Value("${master.client.pool}") int pool,
                                 @Value("${master.name}") String masterName){
        ActorRef router1 = system.actorOf(new RoundRobinPool(pool).props(Props.create(MasterClientActor.class,system,masterName)), "router");
        return router1;
    }
}
