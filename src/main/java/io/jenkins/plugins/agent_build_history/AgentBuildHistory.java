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
import org.kohsuke.stapler.QueryParameter;
import org.kohsuke.stapler.Stapler;
import org.kohsuke.stapler.StaplerRequest;

@Restricted(NoExternalUse.class)
public class AgentBuildHistory implements Action {

  private static final Logger LOGGER = Logger.getLogger(AgentBuildHistory.class.getName());

  private final Computer computer;
  private int totalPages = 1;

  private static boolean loaded = false;
  
  private static boolean loadingComplete = false;


  static{ //TODO do this on storage dir change and remove old scheduler
    File storageDir = new File(getStorageDir());
    if(!storageDir.exists()){
      LOGGER.info("Creating storage directory at " + getStorageDir());
      storageDir.mkdirs();
    }else {
      LOGGER.info("Storage directory already exists at " + getStorageDir());
    }
    schedulePeriodicCleanup(); // Schedule cleanup
  }

  private static final Map<String, Object> nodeLocks = new HashMap<>(); //TODO remove objects when a node is deleted

  // Helper method to get or create a lock for a specific node
  private static Object getNodeLock(String nodeName) {
    synchronized (nodeLocks) {
      return nodeLocks.computeIfAbsent(nodeName, k -> new Object());
    }
  }

  private static void appendAndIndexExecution(String nodeName, AgentExecution execution, boolean isTemporary) {
    // Append execution to node file
    Object lock = getNodeLock(nodeName);
    synchronized (lock) {
      String fileSuffix = isTemporary ? "_tmp" : "";
      LOGGER.info("Appending execution for job: " + execution.getJobName() + ", build: " + execution.getBuildNumber() + " to node: " + nodeName);
      File file = new File(getStorageDir() + "/" + nodeName + "_executions" + fileSuffix + ".ser");
      try (FileOutputStream fos = new FileOutputStream(file, true); //Use AppendableObjectOutputStream if there is already data, to not rewrite Headers
           ObjectOutputStream oos = file.exists() && file.length() > 0 ?
                   new AppendableObjectOutputStream(fos) : new ObjectOutputStream(fos)) {
        oos.writeObject(execution);
        LOGGER.info("Execution appended successfully.");
      } catch (IOException e) {
        LOGGER.log(Level.WARNING, "Failed to append execution for node " + nodeName, e);
      }

      // Update index for the node
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(getStorageDir() + "/" + nodeName + "_index" + fileSuffix + ".txt", true))) {
        LOGGER.info("Updating index for node: " + nodeName);
        writer.write(execution.getJobName() + "," + execution.getBuildNumber() + "," + execution.getStartTimeInMillis());
        writer.newLine();
        LOGGER.info("Index updated successfully for node: " + nodeName);
      } catch (IOException e) {
        LOGGER.log(Level.WARNING, "Failed to update index for node " + nodeName, e);
      }
    }
  }

//does not directly delete the serialized data, but marks it as deleted
  private static void markExecutionAsDeleted(String nodeName, String jobName, int buildNumber) {
    Object lock = getNodeLock(nodeName);
    synchronized (lock) {
      LOGGER.info("Marking execution as deleted for job: " + jobName + ", build: " + buildNumber + " on node: " + nodeName);
      List<String> indexLines = readIndexFile(nodeName);
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(getStorageDir() + "/" + nodeName + "_index.txt"))) {
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
  }
  //return only non deleted executions
  private static List<String> readIndexFile(String nodeName) {
    Object lock = getNodeLock(nodeName);
    synchronized (lock) {
      List<String> indexLines = new ArrayList<>();
      try (BufferedReader reader = new BufferedReader(new FileReader(getStorageDir() + "/" + nodeName + "_index.txt"))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (!line.endsWith(",DELETED")) { // Skip deleted entries
            indexLines.add(line);
          }
        }
      } catch (IOException e) {
        LOGGER.log(Level.WARNING, "Failed to read index file for node " + nodeName, e);
        return Collections.emptyList();
      }
      return indexLines;
    }
  }

  // Deserialize an AgentExecution from disk
  private static AgentExecution loadAgentExecution(String nodeName, String jobName, int buildNumber) { //TODO make a better search algorithm in this file and not from top to bottom if possible, maybe use the index file first
    Object lock = getNodeLock(nodeName);
    synchronized (lock) {
      LOGGER.info("Loading execution for job: " + jobName + ", build: " + buildNumber + " from node: " + nodeName);
      try (ObjectInputStream ois = new ObjectInputStream(
              new FileInputStream(getStorageDir() + "/" + nodeName + "_executions.ser"))) {
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
  }

  private static void schedulePeriodicCleanup() {
    Timer.get().scheduleAtFixedRate(() -> {
      Set<String> nodeNames = getAllSavedNodeNames();
      for (String nodeName : nodeNames) {
        LOGGER.info("Running periodic cleanup for node: " + nodeName);
        rewriteExecutionFile(nodeName);
      }
    }, getDeleteIntervalInMinutes(), getDeleteIntervalInMinutes(), TimeUnit.MINUTES); // Runs cleanup every 10 minutes
  }

  private static void rewriteExecutionFile(String nodeName) {
    Object lock = getNodeLock(nodeName);
    synchronized (lock) {
      List<String> indexLines = readIndexFile(nodeName);

      int currentBatchStart = 0;

      while (currentBatchStart < indexLines.size()) {
        List<AgentExecution> batchExecutions = new ArrayList<>();
        int currentBatchEnd = Math.min(currentBatchStart + getBatchSize(), indexLines.size());
        List<String> batchLines = indexLines.subList(currentBatchStart, currentBatchEnd);

        // Read and process the current batch
        for (String line : batchLines) {
          if (!line.endsWith(",DELETED")) {
            String[] parts = line.split(",");
            String jobName = parts[0];
            int buildNumber = Integer.parseInt(parts[1]);
            AgentExecution execution = loadAgentExecution(nodeName, jobName, buildNumber);
            if (execution != null) {
              batchExecutions.add(execution);
            }
          }
        }
        // Write each execution in the batch to a temporary file
        for (AgentExecution execution : batchExecutions) {
          appendAndIndexExecution(nodeName, execution, true); // true to write to temp files
        }

        currentBatchStart = currentBatchEnd;
      }

      // Replace the original files with the temporary files
      File tempFile = new File(getStorageDir() + "/" + nodeName + "_executions_tmp.ser");
      File originalFile = new File(getStorageDir() + "/" + nodeName + "_executions.ser");
      if (!tempFile.renameTo(originalFile)) {
        LOGGER.log(Level.WARNING, "Failed to replace the original executions file for node " + nodeName);
      }

      File tempIndexFile = new File(getStorageDir() + "/" + nodeName + "_index_tmp.txt");
      File originalIndexFile = new File(getStorageDir() + "/" + nodeName + "_index.txt");
      if (!tempIndexFile.renameTo(originalIndexFile)) {
        LOGGER.log(Level.WARNING, "Failed to replace the original index file for node " + nodeName);
      }
    }
  }

  private static Set<String> getAllSavedNodeNames() {
    Set<String> nodeNames = new HashSet<>();

    File storageDir = new File(getStorageDir());
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
          Object lock = getNodeLock(nodeName);
          synchronized (lock) {
            List<String> indexLines = readIndexFile(nodeName);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(getStorageDir() + "/" + nodeName + "_index.txt"))) {
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
  }


  @Extension
  public static class HistoryNodeListener extends NodeListener {

    @Override
    protected void onDeleted(@NonNull Node node) {
      String nodeName = node.getNodeName();
      Object lock = getNodeLock(nodeName);
      synchronized (lock) {
        // Delete the index file for the node
        File indexFile = new File(getStorageDir() + "/" + nodeName + "_index.txt");
        if (indexFile.exists() && !indexFile.delete()) {
          LOGGER.log(Level.WARNING, "Failed to delete index file for node: " + nodeName);
        }

        // Delete the serialized executions file for the node
        File executionsFile = new File(getStorageDir() + "/" + nodeName + "_executions.ser");
        if (executionsFile.exists() && !executionsFile.delete()) {
          LOGGER.log(Level.WARNING, "Failed to delete executions file for node: " + nodeName);
        }

        LOGGER.log(Level.INFO, () -> "Removed all execution data for deleted node: " + nodeName);
      }
    }
  }

  // Helper method to check if there is existing content in STORAGE_DIR
  private boolean hasExistingContent() {
    File storageDir = new File(getStorageDir());
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

  public int getTotalPages() {
    return totalPages;
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
        loadingComplete = true;
      }
    }
    RunListTable runListTable = new RunListTable(computer.getName());
    //Get Parameters from URL
    StaplerRequest req = Stapler.getCurrentRequest();
    int page = req.getParameter("page") != null ? Integer.parseInt(req.getParameter("page")) : 1;
    int pageSize = req.getParameter("pageSize") != null ? Integer.parseInt(req.getParameter("pageSize")) : getEntriesPerPage();
    String sortColumn = req.getParameter("sortColumn") != null ? req.getParameter("sortColumn") : getDefaultSortColumn();
    String sortOrder = req.getParameter("sortOrder") != null ? req.getParameter("sortOrder") : getDefaultSortOrder();
    //Update totalPages depending on pageSize
    int totalEntries = readIndexFile(computer.getName()).size();
    totalPages = (int) Math.ceil((double) totalEntries / pageSize);

    LOGGER.info("Getting runs for node: " + computer.getName() + " page: " + page + " pageSize: " + pageSize + " sortColumn: " + sortColumn + " sortOrder: " + sortOrder);

    int start = (page-1)*pageSize;
    runListTable.setRuns(getExecutionsForNode(computer.getName(), start, pageSize, sortColumn, sortOrder));
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
          appendAndIndexExecution(node.getNodeName(), execution, false);
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
                appendAndIndexExecution(nodeName, execution, false);
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

//  /* Use by jelly */
//  @Deprecated
//  public Set<AgentExecution> getExecutions() {
//    // Get the node name associated with this AgentBuildHistory instance
//    String nodeName = computer.getName();
//
//    // Read the index file for this node to get all execution entries
//    List<String> indexLines = readIndexFile(nodeName);
//
//    Set<AgentExecution> executions = new TreeSet<>((exec1, exec2) -> {
//      // Sort by start time, and by full display name if start times are equal
//      int compare = Long.compare(exec1.getRun().getStartTimeInMillis(), exec2.getRun().getStartTimeInMillis());
//      if (compare == 0) {
//        return exec1.getRun().getFullDisplayName().compareToIgnoreCase(exec2.getRun().getFullDisplayName());
//      }
//      return compare;
//    });
//
//    // Load each execution from disk based on the index entries
//    for (String line : indexLines) {
//      String[] parts = line.split(",");
//      String jobName = parts[0];
//      int buildNumber = Integer.parseInt(parts[1]);
//
//      // Load the AgentExecution from the serialized file
//      AgentExecution execution = loadAgentExecution(nodeName, jobName, buildNumber);
//      if (execution != null) {
//        executions.add(execution);
//      }
//    }
//
//    return Collections.unmodifiableSet(executions);
//  }

  public List<AgentExecution> getExecutionsForNode(String nodeName, int start, int limit, String sortColumn, String sortOrder) {
    List<AgentExecution> result = new ArrayList<>();
    List<String> indexLines = readIndexFile(nodeName);
    if (indexLines.isEmpty()) {
      return result;
    }

    // Sort index lines based on start time (or other criteria)
    indexLines.sort((a, b) -> {
      int comparison = 0;
      switch(sortColumn){
        case "startTime":
          long timeA = Long.parseLong(a.split(",")[2]);
          long timeB = Long.parseLong(b.split(",")[2]);
          comparison = Long.compare(timeA, timeB);
          break;
        case "build":
          comparison = a.split(",")[0].compareTo(b.split(",")[0]);
          if (comparison == 0) {
            // Only compare build numbers if the job names are the same
            int buildNumberA = Integer.parseInt(a.split(",")[1]);
            int buildNumberB = Integer.parseInt(b.split(",")[1]);
            comparison = Integer.compare(buildNumberA, buildNumberB);
          }
          break;
        default:
          comparison = 0;
      }
        return sortOrder.equals("asc") ? comparison : -comparison;
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
    appendAndIndexExecution(c.getName(), execution, false);
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
    appendAndIndexExecution(nodeName, exec, false);//TODO is it writing a new entry? or updating the existing one? If writing new at least mark the old one as Deleted
  }

  private static int getBatchSize(){
    return AgentBuildHistoryConfig.get().getDeleteBatchSize();
  }

  public static int getEntriesPerPage(){
    return AgentBuildHistoryConfig.get().getEntriesPerPage();
  }

  private static int getDeleteIntervalInMinutes(){
    return AgentBuildHistoryConfig.get().getDeleteIntervalInMinutes();
  }

  public static String getDefaultSortColumn(){
    return AgentBuildHistoryConfig.get().getDefaultSortColumn();
  }

  public static String getDefaultSortOrder(){
    return AgentBuildHistoryConfig.get().getDefaultSortOrder();
  }

  private static String getStorageDir(){
    return AgentBuildHistoryConfig.get().getStorageDir();
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
