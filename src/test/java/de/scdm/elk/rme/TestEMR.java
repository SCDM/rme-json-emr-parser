package de.scdm.elk.rme;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

import de.scdm.elk.aws.Configuration;

public class TestEMR {
  // EMR account

  @Test
  public void testValuation() throws Exception {
    Configuration.INSTANCE.getBUCKET();

    final AWSCredentials credentials = new BasicAWSCredentials(
        Configuration.INSTANCE.getEMR_AWS_ACCESS_KEY_ID(),
        Configuration.INSTANCE.getEMR_AWS_SECRET_ACCESS_KEY());

    final AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(
        credentials);

    emr.setEndpoint("elasticmapreduce.eu-central-1.amazonaws.com");

    final Application sparkApp = new Application().withName("Spark");

    final List<Application> myApps = new ArrayList<Application>();
    myApps.add(sparkApp);

    final HadoopJarStepConfig sparkStepConf = new HadoopJarStepConfig()
        .withJar("command-runner.jar").withArgs("spark-submit", "--deploy-mode",
            "cluster", "--class", "de.scdm.elk.rme.EmrParser",
            Configuration.INSTANCE.getEMR_APPLICATION_PATH()
                + "rme-json-emr-parser-jar-with-dependencies-0.6.jar");

    final StepConfig sparkStep = new StepConfig()
        .withName("Spark Step Command Runner")
        .withActionOnFailure(ActionOnFailure.TERMINATE_CLUSTER)
        .withHadoopJarStep(sparkStepConf);
    //
    // final RunJobFlowRequest request = new RunJobFlowRequest()
    // .withName("Automated Spark Cluster Restart").withVisibleToAllUsers(true)
    // .withApplications(myApps).withReleaseLabel("emr-4.4.0")
    // .withServiceRole("EMR_DefaultRole")
    // .withJobFlowRole("EMR_EC2_DefaultRole")
    // .withLogUri(Configuration.INSTANCE.getEMR_LOG_PATH())
    // .withInstances(new JobFlowInstancesConfig().withEc2KeyName("hadoop")
    // .withInstanceCount(1).withKeepJobFlowAliveWhenNoSteps(false)
    // .withMasterInstanceType("m3.xlarge")
    // .withSlaveInstanceType("m3.xlarge"))
    // .withSteps(sparkStep);
    // final RunJobFlowResult result = emr.runJobFlow(request);

    final AddJobFlowStepsRequest request = new AddJobFlowStepsRequest();
    request.withJobFlowId("j-2ZZWZ17WBEFH9");
    final List<StepConfig> stepConfigs = new ArrayList<StepConfig>();
    request.withSteps(sparkStep);
    AddJobFlowStepsResult addJobFlowSteps = emr.addJobFlowSteps(request);
    System.out.println("Step attached to cluster ID: " + "j-2ZZWZ17WBEFH9");

    System.out.println(addJobFlowSteps.getStepIds());

  }

}
