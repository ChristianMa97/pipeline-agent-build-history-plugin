package io.jenkins.plugins.agent_build_history;

import hudson.Extension;
import hudson.util.ListBoxModel;
import jenkins.model.GlobalConfiguration;
import org.kohsuke.stapler.DataBoundSetter;

@Extension
public class AgentBuildHistoryConfig extends GlobalConfiguration {

    private String storageDir = "/var/jenkins_home/serialized_data";
    private int deleteBatchSize = 20;
    private int entriesPerPage = 20;
    private String defaultSortColumn = "startTime";
    private String defaultSortOrder = "desc";
    private int deleteIntervalInMinutes = 60;

    public AgentBuildHistoryConfig() {
        load(); // Load the persisted configuration
    }

    public String getStorageDir() {
        return storageDir;
    }

    @DataBoundSetter
    public void setStorageDir(String storageDir) {//TODO clean old storage dir and set loaded and cleanup to false
        this.storageDir = storageDir;
        save(); // Save the configuration
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

