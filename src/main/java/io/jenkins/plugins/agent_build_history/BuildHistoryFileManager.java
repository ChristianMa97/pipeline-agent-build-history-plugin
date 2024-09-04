package io.jenkins.plugins.agent_build_history;

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

    public static void appendAndIndexExecution(String nodeName, AgentExecution execution, String storageDir) {
        appendAndIndexExecution(nodeName, execution, storageDir, false);
    }

    // Appends a new execution entry and updates the index
    private static void appendAndIndexExecution(String nodeName, AgentExecution execution, String storageDir, boolean isTemporary) {
        Object lock = getNodeLock(nodeName);
        synchronized (lock) {
            String fileSuffix = isTemporary ? "_tmp" : "";
            File file = new File(storageDir + "/" + nodeName + "_executions" + fileSuffix + ".ser");
            try (FileOutputStream fos = new FileOutputStream(file, true);
                 ObjectOutputStream oos = file.exists() && file.length() > 0 ? new AppendableObjectOutputStream(fos) : new ObjectOutputStream(fos)) {
                oos.writeObject(execution);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to append execution for node " + nodeName, e);
            }

            // Update index for the node
            File indexFile = new File(storageDir + "/" + nodeName + "_index" + fileSuffix + ".txt");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(indexFile, true))) {
                writer.write(execution.getJobName() + "," + execution.getBuildNumber() + "," + execution.getStartTimeInMillis());
                writer.newLine();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to update index for node " + nodeName, e);
            }
        }
    }

    // Mark an execution as deleted in the index file
    public static void markExecutionAsDeleted(String nodeName, String jobName, int buildNumber, String storageDir) {
        Object lock = getNodeLock(nodeName);
        synchronized (lock) {
            List<String> indexLines = readIndexFile(nodeName, storageDir);
            File indexFile = new File(storageDir + "/" + nodeName + "_index.txt");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(indexFile))) {
                for (String line : indexLines) {
                    if (line.startsWith(jobName + "," + buildNumber + ",")) {
                        writer.write(line + ",DELETED");
                    } else {
                        writer.write(line);
                    }
                    writer.newLine();
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to mark execution as deleted for node " + nodeName, e);
            }
        }
    }

    // Load an AgentExecution by deserializing from disk
    public static AgentExecution loadAgentExecution(String nodeName, String jobName, int buildNumber, String storageDir) {
        Object lock = getNodeLock(nodeName);
        synchronized (lock) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(storageDir + "/" + nodeName + "_executions.ser"))) {
                while (true) {
                    try {
                        AgentExecution execution = (AgentExecution) ois.readObject();
                        if (execution.getJobName().equals(jobName) && execution.getBuildNumber() == buildNumber) {
                            return execution;
                        }
                    } catch (EOFException eof) {
                        break; // End of file reached
                    }
                }
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

    // Method for rewriting execution files as part of cleanup
    public static void rewriteExecutionFile(String nodeName, String storageDir, int batchSize) {
        Object lock = getNodeLock(nodeName);
        synchronized (lock) {
            List<String> indexLines = readIndexFile(nodeName, storageDir);
            int currentBatchStart = 0;

            while (currentBatchStart < indexLines.size()) {
                List<AgentExecution> batchExecutions = new ArrayList<>();
                int currentBatchEnd = Math.min(currentBatchStart + batchSize, indexLines.size());
                List<String> batchLines = indexLines.subList(currentBatchStart, currentBatchEnd);

                for (String line : batchLines) {
                    if (!line.endsWith(",DELETED")) {
                        String[] parts = line.split(",");
                        String jobName = parts[0];
                        int buildNumber = Integer.parseInt(parts[1]);
                        AgentExecution execution = loadAgentExecution(nodeName, jobName, buildNumber, storageDir);
                        if (execution != null) {
                            batchExecutions.add(execution);
                        }
                    }
                }
                for (AgentExecution execution : batchExecutions) {
                    appendAndIndexExecution(nodeName, execution, storageDir, true);
                }
                currentBatchStart = currentBatchEnd;
            }

            File tempFile = new File(storageDir + "/" + nodeName + "_executions_tmp.ser");
            File originalFile = new File(storageDir + "/" + nodeName + "_executions.ser");
            if (!tempFile.renameTo(originalFile)) {
                LOGGER.log(Level.WARNING, "Failed to replace the original executions file for node " + nodeName);
            }

            File tempIndexFile = new File(storageDir + "/" + nodeName + "_index_tmp.txt");
            File originalIndexFile = new File(storageDir + "/" + nodeName + "_index.txt");
            if (!tempIndexFile.renameTo(originalIndexFile)) {
                LOGGER.log(Level.WARNING, "Failed to replace the original index file for node " + nodeName);
            }
        }
    }

    // Delete all files associated with a node
    public static void deleteNodeFiles(String nodeName, String storageDir) {
        Object lock = getNodeLock(nodeName);
        synchronized (lock) {
            File indexFile = new File(storageDir + "/" + nodeName + "_index.txt");
            if (indexFile.exists() && !indexFile.delete()) {
                LOGGER.log(Level.WARNING, "Failed to delete index file for node: " + nodeName);
            }

            File executionsFile = new File(storageDir + "/" + nodeName + "_executions.ser");
            if (executionsFile.exists() && !executionsFile.delete()) {
                LOGGER.log(Level.WARNING, "Failed to delete executions file for node: " + nodeName);
            }

            LOGGER.log(Level.INFO, () -> "Removed all execution data for deleted node: " + nodeName);
        }
    }
}
