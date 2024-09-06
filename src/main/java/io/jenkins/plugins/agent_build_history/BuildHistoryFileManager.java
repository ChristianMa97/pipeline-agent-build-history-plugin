package io.jenkins.plugins.agent_build_history;

import org.jenkinsci.plugins.workflow.flow.FlowExecution;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BuildHistoryFileManager {

    private static final Logger LOGGER = Logger.getLogger(BuildHistoryFileManager.class.getName());

    // Handle locks for different nodes
    private static final Map<String, Object> nodeLocks = new HashMap<>();//TODO remove objects when a node is deleted

    public static Object getNodeLock(String nodeName) {
        synchronized (nodeLocks) {
            return nodeLocks.computeIfAbsent(nodeName, k -> new Object());
        }
    }

    // Reads the index file for a given node and returns the list of non-deleted lines
    public static List<String> readIndexFile(String nodeName, String storageDir) {
        Object lock = getNodeLock(nodeName);
        synchronized (lock) {
            List<String> indexLines = new ArrayList<>();
            File indexFile = new File(storageDir + "/" + nodeName + "_index.txt");
            try (BufferedReader reader = new BufferedReader(new FileReader(indexFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                        indexLines.add(line);
                }
            }catch (FileNotFoundException e) {
                LOGGER.log(Level.WARNING, "Index file not found for node " + nodeName);
                return Collections.emptyList();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to read index file for node " + nodeName, e);
                return Collections.emptyList();
            }
            return indexLines;
        }
    }

    // Appends a new execution entry and updates the index
    public static void serializeExecution(String nodeName, AgentExecution execution, String storageDir) { //TODO see if the job build combo is in the index file already if so, don't append it again
        Object lock = getNodeLock(nodeName);
        synchronized (lock) {
            String fileName = nodeName + "_" + execution.getJobName() + "_" + execution.getBuildNumber() + ".ser";

            File file = new File(storageDir + "/" + fileName);
            boolean alreadySerialized = file.exists();
            try (FileOutputStream fos = new FileOutputStream(file);
                 ObjectOutputStream oos =  new ObjectOutputStream(fos)) {
                oos.writeObject(execution);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to append execution for node " + nodeName, e);
            }


            // Update index for the node
            File indexFile = new File(storageDir + "/" + nodeName + "_index" + ".txt");
            if (!alreadySerialized) { //If it was serialized before already, it is contained in the index File already
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(indexFile, true))) {
                    writer.write(execution.getJobName() + "," + execution.getBuildNumber() + "," + execution.getStartTimeInMillis());
                    writer.newLine();
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Failed to update index for node " + nodeName, e);
                }
            }
        }
    }

    // Load an AgentExecution by deserializing from disk
    public static AgentExecution loadAgentExecution(String nodeName, String jobName, int buildNumber, String storageDir) {
        Object lock = getNodeLock(nodeName);
        synchronized (lock) {
            String fileName = nodeName + "_" + jobName + "_" + buildNumber + ".ser";
            File executionFile = new File(storageDir + "/" + fileName);
            if (!executionFile.exists()) {
                return null;
            }
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(executionFile))) {
                AgentExecution execution = (AgentExecution) ois.readObject();
                    LOGGER.log(Level.INFO, () -> "Loaded execution for node " + nodeName + " job " + jobName + " build " + buildNumber +" with " + execution.getFlowNodes().size() + " flow nodes");
                    return execution;
            } catch (IOException | ClassNotFoundException e) {
                LOGGER.log(Level.WARNING, "Failed to load execution for node " + nodeName, e);
            }
            return null;
        }
    }

    // Method for getting all saved node names
    public static Set<String> getAllSavedNodeNames(String storageDir) {
        Set<String> nodeNames = new HashSet<>();
        File storageDirectory = new File(storageDir);
        File[] files = storageDirectory.listFiles((dir, name) -> name.endsWith("_index.txt"));

        if (files != null) {
            for (File file : files) {
                String fileName = file.getName();
                String nodeName = fileName.substring(0, fileName.length() - "_index.txt".length());
                nodeNames.add(nodeName);
            }
        }
        return nodeNames;
    }


    public static void deleteExecution(String nodeName, String jobName, int buildNumber, String storageDir) {
        Object lock = getNodeLock(nodeName);
        synchronized (lock) {
            String fileName = nodeName + "_" + jobName + "_" + buildNumber + ".ser";
            File executionFile = new File(storageDir + "/" + fileName);
            if (executionFile.exists()) {
                if (!executionFile.delete()) {
                    LOGGER.log(Level.WARNING, "Failed to delete execution file: " + executionFile.getName());
                } else {
                    LOGGER.log(Level.INFO, "Deleted execution file: " + executionFile.getName());
                }
                List<String> indexLines = readIndexFile(nodeName, storageDir);
                File indexFile = new File(storageDir + "/" + nodeName + "_index.txt");
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(indexFile))) {
                    for (String line : indexLines) {
                        if (!line.startsWith(jobName + "," + buildNumber + ",")) {
                            writer.write(line);
                            writer.newLine();
                        }
                    }
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Failed to mark execution as deleted for node " + nodeName, e);
                }
            }
        }
    }

    public static void deleteJobSerialization(String jobName, String storageDir){
        Set<String> nodeNames = getAllSavedNodeNames(storageDir);
        for (String nodeName : nodeNames) {
            Object lock = getNodeLock(nodeName);
            synchronized (lock) {
                List<String> indexLines = readIndexFile(nodeName, storageDir);
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(storageDir + "/" + nodeName + "_index.txt"))) {
                    for (String line : indexLines) {
                        if (line.startsWith(jobName + ",")) {
                            //delete this file
                            String[] parts = line.split(",");
                            String fileName = nodeName + "_" + parts[0] + "_" + parts[1] + ".ser";
                            File executionFile = new File(storageDir + "/" + fileName);
                            if (executionFile.exists()) {
                                if (!executionFile.delete()) {
                                    LOGGER.log(Level.WARNING, "Failed to delete execution file: " + executionFile.getName());
                                } else {
                                    LOGGER.log(Level.INFO, "Deleted execution file: " + executionFile.getName());
                                }
                            }
                        } else {
                            writer.write(line);
                            writer.newLine();
                        }
                    }
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Failed to update index for node " + nodeName, e);
                }
            }
        }

    }

    // Delete all files associated with a node
    public static void deleteNodeSerializations(String nodeName, String storageDir) {
        Object lock = getNodeLock(nodeName);
        synchronized (lock) {
            File indexFile = new File(storageDir + "/" + nodeName + "_index.txt");
            if (indexFile.exists() && !indexFile.delete()) {
                LOGGER.log(Level.WARNING, "Failed to delete index file for node: " + nodeName);
            }
            File dir = new File(storageDir);
            File[] files = dir.listFiles((d, name) -> name.startsWith(nodeName + "_") && name.endsWith(".ser"));
            if (files != null) {
                for (File file : files) {
                    if (!file.delete()) {
                        LOGGER.log(Level.WARNING, "Failed to delete execution file: " + file.getName());
                    }
                }
            }
            LOGGER.log(Level.INFO, () -> "Removed all execution data for deleted node: " + nodeName);
        }
    }

    public static void renameNodeFiles(String oldNodeName, String newNodeName, String storageDir) { //TODO change serialized Data as well.
        Object lock = getNodeLock(oldNodeName);
        synchronized (lock) {
            File oldIndexFile = new File(storageDir + "/" + oldNodeName + "_index.txt");
            List<String> lines = readIndexFile(oldNodeName, storageDir);
            for (String line : lines){
                String jobName = line.split(",")[0];
                int buildNumber = Integer.parseInt(line.split(",")[1]);
                AgentExecution execution = loadAgentExecution(oldNodeName, jobName, buildNumber, storageDir);
                if (execution != null) {
                    for (AgentExecution.FlowNodeExecution flowNodeExecution: execution.getFlowNodes()){
                        flowNodeExecution.setNodeName(newNodeName);
                    }
                    serializeExecution(newNodeName, execution, storageDir);
                    File oldExecutionFile = new File(storageDir + "/" + oldNodeName + "_" + jobName + "_" + buildNumber + ".ser");
                    if (oldExecutionFile.exists() && !oldExecutionFile.delete()) {
                        LOGGER.log(Level.WARNING, "Failed to delete execution file: " + oldExecutionFile.getName());
                    }
                }
            }
            if (!oldIndexFile.delete()){
                LOGGER.log(Level.WARNING, "Failed to delete old index file for node: " + oldNodeName);
            }
            LOGGER.log(Level.INFO, () -> "Renamed all execution data for node: " + oldNodeName + " to " + newNodeName);
        }
    }

    public static void renameJob(String oldFullName, String newFullName, String storageDir) { //TODO change serialization Data as well.
        Set<String> nodeNames = getAllSavedNodeNames(storageDir);
        for (String nodeName : nodeNames) {
            Object lock = getNodeLock(nodeName);
            synchronized (lock) {
                List<String> indexLines = readIndexFile(nodeName, storageDir);
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(storageDir + "/" + nodeName + "_index.txt"))) {
                    for (String line : indexLines) {
                        if (line.startsWith(oldFullName + ",")) {
                            AgentExecution execution = loadAgentExecution(nodeName, oldFullName, Integer.parseInt(line.split(",")[1]), storageDir);
                            if (execution != null) {
                                execution.setJobName(newFullName);
                                serializeExecution(nodeName, execution, storageDir); //TODO testen ob das funktioniert mit zwei bufferedWritern zur selben Zeit
                                File oldExecutionFile = new File(storageDir + "/" + nodeName + "_" + oldFullName + "_" + line.split(",")[1] + ".ser");
                                if(!oldExecutionFile.delete()){
                                    LOGGER.log(Level.WARNING, "Failed to delete execution file: " + oldExecutionFile.getName());
                                }
                            }
                        } else {
                            writer.write(line);
                            writer.newLine();
                        }
                    }
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Failed to update index for node " + nodeName, e);
                }
            }
        }
    }
}
