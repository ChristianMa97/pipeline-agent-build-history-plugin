package io.jenkins.plugins.agent_build_history;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.model.AbstractBuild;
import hudson.model.Action;
import hudson.model.Computer;
import hudson.model.Job;
import hudson.model.Node;
import hudson.model.Run;
import hudson.util.RunList;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import jenkins.model.Jenkins;
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
import org.kohsuke.stapler.Stapler;
import org.kohsuke.stapler.StaplerRequest;

@Restricted(NoExternalUse.class)
public class AgentBuildHistory implements Action {

  private static final Logger LOGGER = Logger.getLogger(AgentBuildHistory.class.getName());
  private final Computer computer;
  private int totalPages = 1;
  private static boolean loaded = false;
  private static boolean loadingComplete = false;

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
  /*
   * used by jelly
   */
  public boolean isLoadingComplete() {
    return loadingComplete;
  }

  public static void setLoaded(boolean loaded) {
    AgentBuildHistory.loaded = loaded;
    AgentBuildHistory.loadingComplete = loaded;
  }

  public int getTotalPages() {
    return totalPages;
  }

  // Helper method to check if there is existing content in STORAGE_DIR
  private boolean hasExistingContent() {
    String storageDir = AgentBuildHistoryConfig.get().getStorageDir();
    File dir = new File(storageDir);
    File[] files = dir.listFiles((d, name) -> name.endsWith(".ser") || name.endsWith("_index.txt"));
    return files != null && files.length > 0;
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
    int pageSize = req.getParameter("pageSize") != null ? Integer.parseInt(req.getParameter("pageSize")) : AgentBuildHistoryConfig.get().getEntriesPerPage();
    String sortColumn = req.getParameter("sortColumn") != null ? req.getParameter("sortColumn") : AgentBuildHistoryConfig.get().getDefaultSortColumn();
    String sortOrder = req.getParameter("sortOrder") != null ? req.getParameter("sortOrder") : AgentBuildHistoryConfig.get().getDefaultSortOrder();
    //Update totalPages depending on pageSize
    int totalEntries = BuildHistoryFileManager.readIndexFile(computer.getName(), AgentBuildHistoryConfig.get().getStorageDir()).size();
    totalPages = (int) Math.ceil((double) totalEntries / pageSize);

    LOGGER.info("Getting runs for node: " + computer.getName() + " page: " + page + " pageSize: " + pageSize + " sortColumn: " + sortColumn + " sortOrder: " + sortOrder);

    int start = (page-1)*pageSize;
    runListTable.setRuns(getExecutionsForNode(computer.getName(), start, pageSize, sortColumn, sortOrder));
    return  runListTable;
  }

  public List<AgentExecution> getExecutionsForNode(String nodeName, int start, int limit, String sortColumn, String sortOrder) {
    String storageDir = AgentBuildHistoryConfig.get().getStorageDir();
    List<String> indexLines = BuildHistoryFileManager.readIndexFile(nodeName, storageDir);
    if (indexLines.isEmpty()) {
      return List.of();
    }
    // Sort index lines based on start time or build
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
    List<AgentExecution> result = new ArrayList<>();

    for (String line : page) {
      String[] parts = line.split(",");
      String jobName = parts[0];
      int buildNumber = Integer.parseInt(parts[1]);
      // Load execution using deserialization
      AgentExecution execution = BuildHistoryFileManager.loadAgentExecution(nodeName, jobName, buildNumber, storageDir);
      if (execution != null) {
        result.add(execution);
      }
    }
    return result;
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
          BuildHistoryFileManager.serializeExecution(node.getNodeName(), execution, AgentBuildHistoryConfig.get().getStorageDir());
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
                BuildHistoryFileManager.serializeExecution(nodeName, execution, AgentBuildHistoryConfig.get().getStorageDir());
              }
            }
          }
        }
      }
    });
    loadingComplete = true;
    LOGGER.log(Level.INFO, () -> "Loading all runs complete");
  }

  public static void startJobExecution(Computer c, Run<?, ?> run) {
    AgentExecution execution = new AgentExecution(run);
    BuildHistoryFileManager.serializeExecution(c.getName(), execution, AgentBuildHistoryConfig.get().getStorageDir());
  }

  public static void startFlowNodeExecution(Computer c, WorkflowRun run, FlowNode node) {
    String jobName = run.getParent().getFullName();
    int buildNumber = run.getNumber();
    String nodeName = c.getName();

    // Attempt to load the existing AgentExecution from disk
    AgentExecution exec = BuildHistoryFileManager.loadAgentExecution(nodeName, jobName, buildNumber, AgentBuildHistoryConfig.get().getStorageDir());

    if (exec == null) {
      // If no existing AgentExecution, create a new one
      exec = new AgentExecution(run);
    }
    // Add the FlowNode to the execution (either new or loaded)
    exec.addFlowNode(node, nodeName);

    // Save the updated or new execution back to disk
    BuildHistoryFileManager.serializeExecution(nodeName, exec, AgentBuildHistoryConfig.get().getStorageDir());
  }
  /*
  used by jelly
   */
  public int getEntriesPerPage() {
    return AgentBuildHistoryConfig.get().getEntriesPerPage();
  }
  /*
    used by jelly
   */
  public String getDefaultSortColumn() {
    return AgentBuildHistoryConfig.get().getDefaultSortColumn();
  }
  /*
    used by jelly
   */
  public String getDefaultSortOrder() {
    return AgentBuildHistoryConfig.get().getDefaultSortOrder();
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
