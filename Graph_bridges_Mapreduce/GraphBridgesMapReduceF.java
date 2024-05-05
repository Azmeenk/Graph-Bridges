import java.io.IOException;
import java.util.*;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.DistributedCache;

public class GraphBridgesMapReduce {

    public static class BridgeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Path[] localFiles;  // Store the distributed file paths
        private Map<String, List<String>> adjacencyList = new HashMap<>();  // Store the adjacency list

        public void configure(JobConf job) {
            try {
                localFiles = DistributedCache.getLocalCacheFiles(job);

                // Read the distributed file (adj list) into the adjacencyList map
                try (BufferedReader reader = new BufferedReader(new FileReader(localFiles[0].toString()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t");
                        String vertex = parts[0];
                        String[] neighborsArray = parts[1].split(",");
                        List<String> neighborsList = new ArrayList<>(Arrays.asList(neighborsArray));
                        adjacencyList.put(vertex, neighborsList);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private static boolean isGraphDisconnected(Map<String, List<String>> adjacencyList,
                                            String removedVertex1, String removedVertex2) {
        // Perform BFS traversal to check if the graph is disconnected after removing the edge

        Map<String, Boolean> visited = new HashMap<>();
        Queue<String> queue = new LinkedList<>();

        // Start BFS from a random vertex, e.g., the first vertex in the graph
        String startVertex = adjacencyList.keySet().iterator().next();

        queue.add(startVertex);
        visited.put(startVertex, true);

        while (!queue.isEmpty()) {
            String currentVertex = queue.poll();
            for (String neighbor : adjacencyList.get(currentVertex)) {
                if ((!currentVertex.equals(removedVertex1) || !neighbor.equals(removedVertex2)) &&
                    (!currentVertex.equals(removedVertex2) || !neighbor.equals(removedVertex1)) &&
                    !visited.containsKey(neighbor)) {
                    visited.put(neighbor, true);
                    queue.add(neighbor);
                }
            }
        }
        /// Check if all vertices have been visited
        if (visited.size() == adjacencyList.size()) {
            return false; // Graph is connected
        } else {
            return true; // Graph is disconnected
        }   
    }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            
            // Parse the input edge
           String[] edgeParts = value.toString().split(" ");
            String vertex1 = edgeParts[0];
            String vertex2 = edgeParts[1];

            //store the original neighbours in the variables
            List<String> originalNeighbors1 = new ArrayList<>(adjacencyList.get(vertex1));
            List<String> originalNeighbors2 = new ArrayList<>(adjacencyList.get(vertex2));

            // Check if removing this edge disconnects the graph
            boolean isDisconnected = isGraphDisconnected(adjacencyList, vertex1, vertex2);

            if (isDisconnected) {
        output.collect(new Text("Bridges"), new Text(vertex1 + "and" + vertex2));
    }
        }
    }

    public static class BridgeReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            // Output key-value pairs as they are
            while (values.hasNext()) {
                output.collect(key, values.next());
	   if (!values.hasNext()) {
                output.collect(new Text("No bridges in graph"), new Text(""));
            }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long time = System.currentTimeMillis();
        // Job 3: Find bridges using the Edges list
        JobConf conf = new JobConf(GraphBridgesMapReduce.class);
        conf.setJobName("GraphBridgesMapReduce-BridgeFinding");

        conf.setMapperClass(BridgeMapper.class);
        conf.setReducerClass(BridgeReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2] + "/Bridges"));

        // Set the distributed file (adj list) as a configuration parameter
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);

        // Run the third job and wait for its completion
        RunningJob job = JobClient.runJob(conf);
        job.waitForCompletion();
        System.out.println("Elapsed time = " + (System.currentTimeMillis() - time) + " ms");
    }
}
