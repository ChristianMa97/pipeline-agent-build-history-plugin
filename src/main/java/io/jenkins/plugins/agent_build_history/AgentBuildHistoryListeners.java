package io.jenkins.plugins.agent_build_history;

import edu.umd.cs.findbugs.annotations.NonNull;
import hudson.Extension;
import hudson.model.*;
import hudson.model.listeners.ItemListener;
import hudson.model.listeners.RunListener;
import jenkins.model.NodeListener;
import org.jenkinsci.plugins.workflow.graph.FlowNode;
import org.jenkinsci.plugins.workflow.job.WorkflowRun;
import org.jenkinsci.plugins.workflow.support.steps.ExecutorStepExecution;
import org.kohsuke.accmod.Restricted;
import org.kohsuke.accmod.restrictions.NoExternalUse;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

@Extension
@Restricted(NoExternalUse.class)
public class AgentBuildHistoryListeners {
    private static final Logger LOGGER = Logger.getLogger(AgentBuildHistoryListeners.class.getName());
    @Extension
    public static class HistoryRunListener extends RunListener<Run<?, ?>> {

        @Override
        public void onDeleted(Run run) {
            String jobName = run.getParent().getFullName();
            int buildNumber = run.getNumber();
            Set<String> nodeNames = BuildHistoryFileManager.getAllSavedNodeNames(AgentBuildHistoryConfig.get().getStorageDir());
            for (String nodeName : nodeNames) {
                BuildHistoryFileManager.markExecutionAsDeleted(nodeName, jobName, buildNumber, AgentBuildHistoryConfig.get().getStorageDir());
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
                Set<String> nodeNames = BuildHistoryFileManager.getAllSavedNodeNames(AgentBuildHistoryConfig.get().getStorageDir());
                for (String nodeName : nodeNames) {
                    Object lock = BuildHistoryFileManager.getNodeLock(nodeName);
                    synchronized (lock) {
                        List<String> indexLines = BuildHistoryFileManager.readIndexFile(nodeName, AgentBuildHistoryConfig.get().getStorageDir());
                        try (BufferedWriter writer = new BufferedWriter(new FileWriter(AgentBuildHistoryConfig.get().getStorageDir() + "/" + nodeName + "_index.txt"))) {
                            for (String line : indexLines) {
                                if (line.startsWith(jobName + ",")) {
                                    writer.write(line + ",DELETED");
                                } else {
                                    writer.write(line);
                                }
                                writer.newLine();
                            }
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
            BuildHistoryFileManager.deleteNodeFiles(nodeName, AgentBuildHistoryConfig.get().getStorageDir());
        }
    }

    @Extension
    public static class AgentBuildHistoryExecutorListener implements ExecutorListener {

        @Override
        public void taskStarted(Executor executor, Queue.Task task) {
            Queue.Executable executable = executor.getCurrentExecutable();
            Computer c = executor.getOwner();
            if (executable instanceof AbstractBuild) {
                Run<?, ?> run = (Run<?, ?>) executable;
                LOGGER.log(Level.FINER, () -> "Starting Job: " + run.getFullDisplayName() + " on " + c.getName());
                AgentBuildHistory.startJobExecution(c, run);
            } else if (task instanceof ExecutorStepExecution.PlaceholderTask) {
                ExecutorStepExecution.PlaceholderTask pht = (ExecutorStepExecution.PlaceholderTask) task;
                executable = task.getOwnerExecutable();
                try {
                    FlowNode node = pht.getNode();
                    if (node != null && executable instanceof WorkflowRun) {
                        Run<?, ?> run = (Run<?, ?>) executable;
                        AgentBuildHistory.startFlowNodeExecution(c, (WorkflowRun) run, node);
                        LOGGER.log(Level.FINER, () -> "Starting part of pipeline: " + run.getFullDisplayName()
                                + " Node id: " + node.getId() + " on " + c.getName());
                    }
                } catch (IOException | InterruptedException e) {
                    LOGGER.log(Level.FINE, e, () -> "Failed to get FlowNode");
                }
            }
        }
    }
}
