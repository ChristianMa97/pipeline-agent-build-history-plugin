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

  private final Computer computer;

  private static boolean loaded = false;
  
  private static boolean loadingComplete = false;

  static{
    File storageDir = new File(STORAGE_DIR);
    if(!storageDir.exists()){
      LOGGER.info("Creating storage directory at " + STORAGE_DIR);
      storageDir.mkdirs();
    }else {
      LOGGER.info("Storage directory already exists at " + STORAGE_DIR);
    }
  }

  private static void appendAndIndexExecution(String nodeName, AgentExecution execution) {
    // Append execution to node file
    LOGGER.info("Appending execution for job: " + execution.getJobName() + ", build: " + execution.getBuildNumber() + " to node: " + nodeName);
    File file = new File(STORAGE_DIR + "/" + nodeName + "_executions.ser");
    try (FileOutputStream fos = new FileOutputStream(file, true); //Use AppendableObjectOutputStream if there is already data, to not rewrite Headers
         ObjectOutputStream oos = file.exists() && file.length() > 0 ?
                 new AppendableObjectOutputStream(fos) : new ObjectOutputStream(fos)) {
      oos.writeObject(execution);
      LOGGER.info("Execution appended successfully.");
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "Failed to append execution for node " + nodeName, e);
    }

    // Update index for the node
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(STORAGE_DIR + "/" + nodeName + "_index.txt", true))) {
      LOGGER.info("Updating index for node: " + nodeName);
      writer.write(execution.getJobName() + "," + execution.getBuildNumber() + "," + execution.getStartTimeInMillis());
      writer.newLine();
      LOGGER.info("Index updated successfully for node: " + nodeName);
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "Failed to update index for node " + nodeName, e);
    }
  }

//does not directly delete the serialized data, but marks it as deleted
  private static void markExecutionAsDeleted(String nodeName, String jobName, int buildNumber) {
    LOGGER.info("Marking execution as deleted for job: " + jobName + ", build: " + buildNumber + " on node: " + nodeName);
    List<String> indexLines = readIndexFile(nodeName);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(STORAGE_DIR + "/" + nodeName + "_index.txt"))) {
      for (String line : indexLines) {
        if (line.startsWith(jobName + "," + buildNumber + ",")) {
          writer.write(line + ",DELETED"); // Mark as deleted
        } else {
          writer.write(line);
        }
        writer.newLine();
      }
      LOGGER.info("Execution marked as deleted for job: " + jobName + ", build: " + buildNumber + " on node: " + nodeName);
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "Failed to update index for node " + nodeName, e);
    }
  }
  //return only non deleted executions
  private static List<String> readIndexFile(String nodeName) {
    List<String> indexLines = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(STORAGE_DIR + "/" + nodeName + "_index.txt"))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.endsWith(",DELETED")) { // Skip deleted entries
          indexLines.add(line);
        }
      }
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "Failed to read index file for node " + nodeName, e);
    }
    return indexLines;
  }

  // Deserialize an AgentExecution from disk
  private static AgentExecution loadAgentExecution(String nodeName, String jobName, int buildNumber) { //TODO make a better search algorithm in this file and not from top to bottom if possible, maybe use the index file first
    LOGGER.info("Loading execution for job: " + jobName + ", build: " + buildNumber + " from node: " + nodeName);
    try (ObjectInputStream ois = new ObjectInputStream(
            new FileInputStream(STORAGE_DIR + "/" + nodeName + "_executions.ser"))) {
      while (true) {
        try {
          AgentExecution execution = (AgentExecution) ois.readObject();
          if (execution.getJobName().equals(jobName) && execution.getBuildNumber() == buildNumber) {
            LOGGER.info("Execution found and loaded for job: " + jobName + ", build: " + buildNumber);
            return execution;
          }
        } catch (EOFException eof) {
          break; // End of file reached
        }
      }
    } catch (IOException | ClassNotFoundException e) {
      LOGGER.log(Level.WARNING, "Failed to load execution for node " + nodeName, e);
    }
    LOGGER.warning("Execution not found for job: " + jobName + ", build: " + buildNumber + " on node: " + nodeName);
    return null;
  }

  private static void rewriteExecutionFile(String nodeName) {
    List<AgentExecution> allExecutions = new ArrayList<>();
    List<String> indexLines = readIndexFile(nodeName);

    // Read all non-deleted executions
    for (String line : indexLines) {
      if (!line.endsWith(",DELETED")) {
        String[] parts = line.split(",");
        String jobName = parts[0];
        int buildNumber = Integer.parseInt(parts[1]);
        AgentExecution execution = loadAgentExecution(nodeName, jobName, buildNumber);
        if (execution != null) {
          allExecutions.add(execution);
        }
      }
    }

    // Write all executions back to the file
    try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(STORAGE_DIR + "/" + nodeName + "_executions.ser"))) {
      for (AgentExecution execution : allExecutions) {
        oos.writeObject(execution);
      }
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "Failed to rewrite executions file for node " + nodeName, e);
    }

    // Update the index file with only non-deleted entries
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(STORAGE_DIR + "/" + nodeName + "_index.txt"))) {
      for (AgentExecution execution : allExecutions) {
        writer.write(execution.getJobName() + "," + execution.getBuildNumber() + "," + execution.getStartTimeInMillis());
        writer.newLine();
      }
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "Failed to update index for node " + nodeName, e);
    }
  }

  private static Set<String> getAllSavedNodeNames() {
    Set<String> nodeNames = new HashSet<>();

    File storageDir = new File(STORAGE_DIR);
    File[] files = storageDir.listFiles((dir, name) -> name.endsWith("_index.txt"));

    if (files != null) {
      for (File file : files) {
        String fileName = file.getName();
        // Extract the node name by removing the "_index.txt" suffix
        String nodeName = fileName.substring(0, fileName.length() - "_index.txt".length());
        nodeNames.add(nodeName);
      }
    }

    return nodeNames;
  }


  @Extension
  public static class HistoryRunListener extends RunListener<Run<?, ?>> {

    @Override
    public void onDeleted(Run run) {
      String jobName = run.getParent().getFullName();
      int buildNumber = run.getNumber();

      Set<String> nodeNames = getAllSavedNodeNames();
      for (String nodeName : nodeNames) {
        markExecutionAsDeleted(nodeName, jobName, buildNumber);
      }
    }
  }

  @Extension
  public static class HistoryItemListener extends ItemListener {

    @Override
    public void onDeleted(Item item) {
      if (item instanceof Job) {
        Job<?, ?> job = (Job<?, ?>) item;
        String jobName = job.getFullName();

        Set<String> nodeNames = getAllSavedNodeNames();
        for (String nodeName : nodeNames) {
          List<String> indexLines = readIndexFile(nodeName);
          try (BufferedWriter writer = new BufferedWriter(new FileWriter(STORAGE_DIR + "/" + nodeName + "_index.txt"))) {
            for (String line : indexLines) {
              if (line.startsWith(jobName + ",")) {
                writer.write(line + ",DELETED"); // Mark as deleted
              } else {
                writer.write(line);
              }
              writer.newLine();
            }
            LOGGER.info("Job marked for deletion: " + jobName);
          } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to update index for node " + nodeName, e);
          }
        }
      }
    }
  }


  @Extension
  public static class HistoryNodeListener extends NodeListener {

    @Override
    protected void onDeleted(@NonNull Node node) {
      String nodeName = node.getNodeName();

      // Delete the index file for the node
      File indexFile = new File(STORAGE_DIR + "/" + nodeName + "_index.txt");
      if (indexFile.exists() && !indexFile.delete()) {
        LOGGER.log(Level.WARNING, "Failed to delete index file for node: " + nodeName);
      }

      // Delete the serialized executions file for the node
      File executionsFile = new File(STORAGE_DIR + "/" + nodeName + "_executions.ser");
      if (executionsFile.exists() && !executionsFile.delete()) {
        LOGGER.log(Level.WARNING, "Failed to delete executions file for node: " + nodeName);
      }

      LOGGER.log(Level.INFO, () -> "Removed all execution data for deleted node: " + nodeName);
    }
  }

  // Helper method to check if there is existing content in STORAGE_DIR
  private boolean hasExistingContent() {
    File storageDir = new File(STORAGE_DIR);
    File[] files = storageDir.listFiles((dir, name) -> name.endsWith("_executions.ser") || name.endsWith("_index.txt"));

    if (files != null) {
      for (File file : files) {
        if (file.length() > 0) { // Check if file has content
          return true;
        }
      }
    }
    return false;
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
      if (!hasExistingContent()) {
        loaded = true;
        Timer.get().schedule(AgentBuildHistory::load, 0, TimeUnit.SECONDS);
      } else {
        loaded = true; // Mark as loaded because we found existing content
      }
    }
    RunListTable runListTable = new RunListTable(computer.getName());
    runListTable.setRuns(getExecutionsForNode(computer.getName(), 0, 100));
    return  runListTable;
  }

  private static void load() {
    LOGGER.log(Level.INFO, () -> "Starting to load all runs");
    RunList<Run<?, ?>> runList = RunList.fromJobs((Iterable) Jenkins.get().allItems(Job.class));
    runList.forEach(run -> {
      LOGGER.info("Loading run " + run.getFullDisplayName());
      AgentExecution execution = new AgentExecution(run);

      if (run instanceof AbstractBuild) {
        Node node = ((AbstractBuild<?, ?>) run).getBuiltOn();
        if (node != null) {
          LOGGER.info("Loading AbstractBuild on node: " + node.getNodeName());
          appendAndIndexExecution(node.getNodeName(), execution);
        }
      } else if (run instanceof WorkflowRun) {
        WorkflowRun wfr = (WorkflowRun) run;
        LOGGER.info("Loading WorkflowRun: " + wfr.getFullDisplayName());
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
                LOGGER.info("Loading WorkflowRun FlowNode on node: " + nodeName);
                appendAndIndexExecution(nodeName, execution);
              }
            }
          }
        }
      }
      // Clear the run object to free memory
      run = null;
    });
    loadingComplete = true;
    LOGGER.log(Level.INFO, () -> "Loading all runs complete");
  }

  /* Use by jelly */
  public Set<AgentExecution> getExecutions() {
    // Get the node name associated with this AgentBuildHistory instance
    String nodeName = computer.getName();

    // Read the index file for this node to get all execution entries
    List<String> indexLines = readIndexFile(nodeName);

    Set<AgentExecution> executions = new TreeSet<>((exec1, exec2) -> {
      // Sort by start time, and by full display name if start times are equal
      int compare = Long.compare(exec1.getRun().getStartTimeInMillis(), exec2.getRun().getStartTimeInMillis());
      if (compare == 0) {
        return exec1.getRun().getFullDisplayName().compareToIgnoreCase(exec2.getRun().getFullDisplayName());
      }
      return compare;
    });

    // Load each execution from disk based on the index entries
    for (String line : indexLines) {
      String[] parts = line.split(",");
      String jobName = parts[0];
      int buildNumber = Integer.parseInt(parts[1]);

      // Load the AgentExecution from the serialized file
      AgentExecution execution = loadAgentExecution(nodeName, jobName, buildNumber);
      if (execution != null) {
        executions.add(execution);
      }
    }

    return Collections.unmodifiableSet(executions);
  }

  public List<AgentExecution> getExecutionsForNode(String nodeName, int start, int limit) {
    List<AgentExecution> result = new ArrayList<>();
    List<String> indexLines = readIndexFile(nodeName); //TODO when loading the first time, it cant find this file and throws an error java.io.FileNotFoundException: handle this case

    // Sort index lines based on start time (or other criteria)
    indexLines.sort((a, b) -> {//TODO change sorting descendant
      long timeA = Long.parseLong(a.split(",")[2]);
      long timeB = Long.parseLong(b.split(",")[2]);
      return Long.compare(timeA, timeB);
    });

    // Apply pagination
    int end = Math.min(start + limit, indexLines.size());
    List<String> page = indexLines.subList(start, end);

    for (String line : page) {
      String[] parts = line.split(",");
      String jobName = parts[0];
      int buildNumber = Integer.parseInt(parts[1]);

      // Load execution using deserialization
      AgentExecution execution = loadAgentExecution(nodeName, jobName, buildNumber);
      if (execution != null) {
        result.add(execution);
      }
    }
    return result;
  }

  public static void startJobExecution(Computer c, Run<?, ?> run) {
    AgentExecution execution = new AgentExecution(run);
    appendAndIndexExecution(c.getName(), execution);
  }

  public static void startFlowNodeExecution(Computer c, WorkflowRun run, FlowNode node) {
    String jobName = run.getParent().getFullName();
    int buildNumber = run.getNumber();
    String nodeName = c.getName();

    // Attempt to load the existing AgentExecution from disk
    AgentExecution exec = loadAgentExecution(nodeName, jobName, buildNumber);

    if (exec == null) {
      // If no existing AgentExecution, create a new one
      exec = new AgentExecution(run);
    }

    // Add the FlowNode to the execution (either new or loaded)
    exec.addFlowNode(node, nodeName);

    // Save the updated or new execution back to disk
    appendAndIndexExecution(nodeName, exec);
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
