package io.jenkins.plugins.agent_build_history;

import hudson.Extension;
import hudson.util.ListBoxModel;
import jenkins.model.GlobalConfiguration;
import jenkins.util.Timer;
import org.kohsuke.stapler.DataBoundSetter;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

@Extension
public class AgentBuildHistoryConfig extends GlobalConfiguration {
    private static final Logger LOGGER = Logger.getLogger(AgentBuildHistoryConfig.class.getName());

    private String storageDir = "/var/jenkins_home/serialized_data";
    private int deleteBatchSize = 20;
    private int entriesPerPage = 20;
    private String defaultSortColumn = "startTime";
    private String defaultSortOrder = "desc";
    private int deleteIntervalInMinutes = 60;

    private static ScheduledFuture<?> cleanupTask; // Reference to the scheduled cleanup task

    public AgentBuildHistoryConfig() {
        load(); // Load the persisted configuration
        ensureStorageDir();
        schedulePeriodicCleanup();
    }

    private void ensureStorageDir() {
        File storageDirectory = new File(storageDir);
        if (!storageDirectory.exists()) {
            LOGGER.info("Creating storage directory at " + storageDir);
            storageDirectory.mkdirs();
        } else {
            LOGGER.info("Storage directory already exists at " + storageDir);
        }
    }

    private void schedulePeriodicCleanup() {
        // Cancel the existing task if it is running
        if (cleanupTask != null && !cleanupTask.isCancelled()) {
            cleanupTask.cancel(true); // Cancel the old task
            LOGGER.info("Canceled existing cleanup task.");
        }
        cleanupTask = Timer.get().scheduleAtFixedRate(() -> {
            Set<String> nodeNames = BuildHistoryFileManager.getAllSavedNodeNames(storageDir);
            for (String nodeName : nodeNames) {
                LOGGER.info("Running periodic cleanup for node: " + nodeName);
                BuildHistoryFileManager.rewriteExecutionFile(nodeName, storageDir, getDeleteBatchSize());
            }
        }, deleteIntervalInMinutes, deleteIntervalInMinutes, TimeUnit.MINUTES);
        LOGGER.info("Scheduled new cleanup task with interval: " + deleteIntervalInMinutes + " minutes.");
    }

    public String getStorageDir() {
        return storageDir;
    }

    @DataBoundSetter
    public void setStorageDir(String storageDir) {
        if (!this.storageDir.equals(storageDir)) {
            LOGGER.info("Changing storage directory from " + this.storageDir + " to " + storageDir);
            this.storageDir = storageDir;
            ensureStorageDir();
            schedulePeriodicCleanup();
            AgentBuildHistory.setLoaded(false);
            save(); // Save the configuration
        }
    }

    public int getDeleteBatchSize() {
        return deleteBatchSize;
    }

    @DataBoundSetter
    public void setDeleteBatchSize(int deleteBatchSize) {
        this.deleteBatchSize = deleteBatchSize;
        save(); // Save the configuration
    }

    public int getEntriesPerPage() {
        return entriesPerPage;
    }

    @DataBoundSetter
    public void setEntriesPerPage(int entriesPerPage) {
        this.entriesPerPage = entriesPerPage;
        save(); // Save the configuration
    }

    public String getDefaultSortColumn() {
        return defaultSortColumn;
    }

    @DataBoundSetter
    public void setDefaultSortColumn(String defaultSortColumn) {
        this.defaultSortColumn = defaultSortColumn;
        save(); // Save the configuration
    }

    public String getDefaultSortOrder() {
        return defaultSortOrder;
    }

    @DataBoundSetter
    public void setDefaultSortOrder(String defaultSortOrder) {
        this.defaultSortOrder = defaultSortOrder;
        save(); // Save the configuration
    }

    // Static method to access the configuration instance
    public static AgentBuildHistoryConfig get() {
        return GlobalConfiguration.all().get(AgentBuildHistoryConfig.class);
    }

    public int getDeleteIntervalInMinutes() {
        return deleteIntervalInMinutes;
    }
    @DataBoundSetter
    public void setDeleteIntervalInMinutes(int deleteIntervalInMinutes) {
        this.deleteIntervalInMinutes = deleteIntervalInMinutes;
        schedulePeriodicCleanup();
        save();
    }

    public ListBoxModel doFillDefaultSortColumnItems() {
        ListBoxModel items = new ListBoxModel();
        items.add("Start Time", "startTime");
        items.add("Build Name and Build Number", "build");
        return items;
    }

    public ListBoxModel doFillDefaultSortOrderItems() {
        ListBoxModel items = new ListBoxModel();
        items.add("Ascending", "asc");
        items.add("Descending", "desc");
        return items;
    }
}

