package io.jenkins.plugins.agent_build_history;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.Extension;
import hudson.model.AbstractBuild;
import hudson.model.Action;
import hudson.model.Computer;
import hudson.model.Item;
import hudson.model.Job;
import hudson.model.Node;
import hudson.model.Run;
import hudson.model.listeners.ItemListener;
import hudson.model.listeners.RunListener;
import hudson.util.RunList;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import jenkins.model.Jenkins;
import jenkins.model.NodeListener;
import jenkins.util.Timer;
import org.jenkinsci.plugins.workflow.cps.nodes.StepStartNode;
import org.jenkinsci.plugins.workflow.flow.FlowExecution;
import org.jenkinsci.plugins.workflow.graph.FlowNode;
import org.jenkinsci.plugins.workflow.graphanalysis.DepthFirstScanner;
import org.jenkinsci.plugins.workflow.job.WorkflowRun;
import org.jenkinsci.plugins.workflow.steps.StepDescriptor;
import org.jenkinsci.plugins.workflow.support.actions.WorkspaceActionImpl;
import org.jenkinsci.plugins.workflow.support.steps.ExecutorStep;
import org.kohsuke.accmod.Restricted;
import org.kohsuke.accmod.restrictions.NoExternalUse;

@Restricted(NoExternalUse.class)
public class AgentBuildHistory implements Action {

  private static final Logger LOGGER = Logger.getLogger(AgentBuildHistory.class.getName());
  private static final String STORAGE_DIR = "/var/jenkins_home/serialized_data";

  private static final Map<String, Set<String>> agentExecutions = new HashMap<>();
  private static final Map<String, String> agentExecutionsMap = new HashMap<>();

  private static final int CACHE_SIZE = 100; // Adjust size as needed
  private static final Map<String, AgentExecution> executionCache = Collections.synchronizedMap(new LinkedHashMap<String, AgentExecution>(CACHE_SIZE, 0.75f, true) {
    @Override
    protected boolean removeEldestEntry(Map.Entry<String, AgentExecution> eldest) {
      return size() > CACHE_SIZE; // Eviction policy: remove eldest if cache is full
    }
  });

  private final Computer computer;

  private static boolean loaded = false;
  
  private static boolean loadingComplete = false;

  static{
    File storageDir = new File(STORAGE_DIR);
    if(!storageDir.exists()){
      storageDir.mkdirs();
    }
  }

  // Serialize an AgentExecution to disk
  private static void saveAgentExecution(String computerName, AgentExecution execution) {
    try (ObjectOutputStream oos = new ObjectOutputStream(
            new FileOutputStream(STORAGE_DIR + "/" + computerName + "_" + execution.getJobName() + "_" + execution.getBuildNumber() + ".ser"))) {
      oos.writeObject(execution);
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "Failed to save AgentExecution for " + computerName, e);
    }
  }

  // Deserialize an AgentExecution from disk
  private static AgentExecution loadAgentExecution(String computerName, String jobName, int buildNumber) {
    try (ObjectInputStream ois = new ObjectInputStream(
            new FileInputStream(STORAGE_DIR + "/" + computerName + "_" + jobName + "_" + buildNumber + ".ser"))) {
      return (AgentExecution) ois.readObject();
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.log(Level.WARNING, "Failed to load AgentExecution for " + computerName, e);
      return null;
    }
  }

  // Utility method to create unique identifiers
  private static String createExecutionIdentifier(String jobName, int buildNumber) {
    return jobName + "_" + buildNumber;
  }

  @Extension
  public static class HistoryRunListener extends RunListener<Run<?, ?>> {

    @Override
    public void onDeleted(Run run) {
      String identifier = createExecutionIdentifier(run.getParent().getName(), run.getNumber());
      for (Set<String> executions : agentExecutions.values()) {
        executions.remove(identifier);
      }
      agentExecutionsMap.remove(identifier);

      // Remove serialized data
      deleteSerializedExecution(run.getParent().getName(), run.getNumber());
    }
  }

  @Extension
  public static class HistoryItemListener extends ItemListener {

    @Override
    public void onDeleted(Item item) {
      if (item instanceof Job) {
        String jobName = item.getFullName();
        for (Set<String> executions : agentExecutions.values()) {
          executions.removeIf(exec -> exec.startsWith(jobName + "_"));
        }
        agentExecutionsMap.keySet().removeIf(key -> key.startsWith(jobName + "_"));

        // Remove all serialized executions for this job
        deleteSerializedExecutionsForJob(jobName);
      }
    }
  }

  // Utility method to delete serialized execution data
  private static void deleteSerializedExecution(String jobName, int buildNumber) {
    String fileName = STORAGE_DIR + "/" + jobName + "_" + buildNumber + ".ser";
    File file = new File(fileName);
    if (file.exists()) {
      if (!file.delete()) {
        LOGGER.log(Level.WARNING, "Failed to delete serialized execution file: " + fileName);
      }
    }
  }

  // Utility method to delete all serialized executions for a job
  private static void deleteSerializedExecutionsForJob(String jobName) {
    File storageDir = new File(STORAGE_DIR);
    File[] files = storageDir.listFiles((dir, name) -> name.startsWith(jobName + "_") && name.endsWith(".ser"));
    if (files != null) {
      for (File file : files) {
        if (!file.delete()) {
          LOGGER.log(Level.WARNING, "Failed to delete serialized execution file: " + file.getAbsolutePath());
        }
      }
    }
  }

  @Extension
  public static class HistoryNodeListener extends NodeListener {

    @Override
    protected void onDeleted(@NonNull Node node) {
      String nodeName = node.getNodeName();
      Set<String> executions = agentExecutions.remove(nodeName); // Remove from memory

      if (executions != null) {
        for (String identifier : executions) {
          String[] parts = identifier.split("_");
          String jobName = parts[0];
          int buildNumber = Integer.parseInt(parts[1]);
          deleteSerializedExecution(jobName, buildNumber); // Delete serialized data
        }
      }

      LOGGER.log(Level.INFO, () -> "Removed all execution data for deleted node: " + nodeName);
    }
  }

  public AgentBuildHistory(Computer computer) {
    this.computer = computer;
    LOGGER.log(Level.INFO, () -> "Creating AgentBuildHistory for " + computer.getName());
  }

  /*
   * used by jelly
   */
  public Computer getComputer() {
    return computer;
  }

  public boolean isLoadingComplete() {
    return loadingComplete;
  }

  @SuppressFBWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  public RunListTable getHandler() {
    if (!loaded) {
      loaded = true;
      Timer.get().schedule(AgentBuildHistory::load, 0, TimeUnit.SECONDS);
    }
    RunListTable runListTable = new RunListTable(computer.getName());
    runListTable.setRuns(getLazyLoadedExecutions());
    return  runListTable;
  }

  private static void load() {
    LOGGER.log(Level.INFO, () -> "Starting to load all runs");
    RunList<Run<?, ?>> runList = RunList.fromJobs((Iterable) Jenkins.get().allItems(Job.class));
    runList.forEach(run -> {
      LOGGER.info("Loading run " + run.getFullDisplayName());
      String identifier = createExecutionIdentifier(run.getParent().getFullName(), run.getNumber());
      AgentExecution execution = getAgentExecution(run);

      if (run instanceof AbstractBuild) {
        Node node = ((AbstractBuild<?, ?>) run).getBuiltOn();
        if (node != null) {
          Set<String> executions = loadExecutions(node.getNodeName());
          executions.add(identifier);
        }
      } else if (run instanceof WorkflowRun) {
        WorkflowRun wfr = (WorkflowRun) run;
        FlowExecution flowExecution = wfr.getExecution();
        if (flowExecution != null) {
          for (FlowNode flowNode : new DepthFirstScanner().allNodes(flowExecution)) {
            if (! (flowNode instanceof StepStartNode)) {
              continue;
            }
            for (WorkspaceActionImpl action : flowNode.getActions(WorkspaceActionImpl.class)) {
              StepStartNode startNode = (StepStartNode) flowNode;
              StepDescriptor descriptor = startNode.getDescriptor();
              if (descriptor instanceof ExecutorStep.DescriptorImpl) {
                String nodeName = action.getNode();
                execution.addFlowNode(flowNode, nodeName);
                Set<String> executions = loadExecutions(nodeName);
                executions.add(identifier);
              }
            }
          }
        }
      }
      // Serialize execution after all modifications
      saveAgentExecution(run.getParent().getFullName(), execution);
      // Clear the run object to free memory
      run = null;
    });
    loadingComplete = true;
    LOGGER.log(Level.INFO, () -> "Loading all runs complete");
  }

  private static Set<String> loadExecutions(String computerName) {
    Set<String> executions = agentExecutions.get(computerName);
    if (executions == null) {
      LOGGER.log(Level.INFO, () -> "Creating executions for computer " + computerName);
      executions = Collections.synchronizedSet(new TreeSet<>());
      agentExecutions.put(computerName, executions);
    }
    return executions;
  }

  // Implement lazy loading for executions
  private Iterable<AgentExecution> getLazyLoadedExecutions() {
    Set<String> executionIdentifiers = agentExecutions.get(computer.getName());
    return () -> new Iterator<AgentExecution>() {
      private final Iterator<String> internalIterator = executionIdentifiers.iterator();

      @Override
      public boolean hasNext() {
        return internalIterator.hasNext();
      }

      @Override
      public AgentExecution next() {
        String identifier = internalIterator.next();
        AgentExecution execution = executionCache.get(identifier);

        // If not in cache, load from disk
        if (execution == null) {
          String[] parts = identifier.split("_");
          String jobName = parts[0];
          int buildNumber = Integer.parseInt(parts[1]);
          execution = loadAgentExecution(computer.getName(), jobName, buildNumber);

          if (execution != null) {
            executionCache.put(identifier, execution); // Cache the loaded execution
          }
        }
        return execution;
      }
    };
  }

  /* use by jelly */
  public Set<AgentExecution> getExecutions() {
    Set<String> executionIdentifiers = agentExecutions.get(computer.getName());
    if (executionIdentifiers == null) {
      return Collections.emptySet();
    }

    Set<AgentExecution> executions = new TreeSet<>();
    for (String identifier : executionIdentifiers) {
      AgentExecution execution = executionCache.get(identifier);
      if (execution == null) {
        // If not in cache, load from disk
        String[] parts = identifier.split("_");
        String jobName = parts[0];
        int buildNumber = Integer.parseInt(parts[1]);
        execution = loadAgentExecution(computer.getName(), jobName, buildNumber);

        if (execution != null) {
          executionCache.put(identifier, execution); // Cache the loaded execution
          executions.add(execution);
        }
      } else {
        executions.add(execution);
      }
    }

    return Collections.unmodifiableSet(executions);
  }

  @NonNull
  private static AgentExecution getAgentExecution(Run<?, ?> run) {
    String identifier = createExecutionIdentifier(run.getParent().getName(), run.getNumber());
    AgentExecution exec = executionCache.get(identifier);
    if (exec == null) {
      LOGGER.log(Level.INFO, () -> "Loading execution for run " + run.getFullDisplayName());
      exec = new AgentExecution(run);
      executionCache.put(identifier, exec);
    }
    return exec;
  }

  public static void startJobExecution(Computer c, Run<?, ?> run) {
    String identifier = createExecutionIdentifier(run.getParent().getName(), run.getNumber());
    AgentExecution execution = new AgentExecution(run);

    // Cache the execution
    executionCache.put(identifier, execution);


    loadExecutions(c.getName()).add(identifier);

    saveAgentExecution(c.getName(), execution);
  }

  public static void startFlowNodeExecution(Computer c, WorkflowRun run, FlowNode node) {
    String identifier = createExecutionIdentifier(run.getParent().getName(), run.getNumber());
    AgentExecution exec = getAgentExecution(run);
    exec.addFlowNode(node, c.getName());

    // Cache the execution
    executionCache.put(identifier, exec);

    loadExecutions(c.getName()).add(identifier);

    saveAgentExecution(c.getName(), exec);
  }

  @Override
  public String getIconFileName() {
    return "symbol-file-tray-stacked-outline plugin-ionicons-api";
  }

  @Override
  public String getDisplayName() {
    return "Extended Build History";
  }

  @Override
  public String getUrlName() {
    return "extendedBuildHistory";
  }
}
