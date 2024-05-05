import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.broadcast.Broadcast;
import java.util.stream.Collectors;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class GraphBridgesSpark {

    public static void main(String[] args) {
        // Checking for correct command-line arguments
        if (args.length != 2) {
            System.err.println("Usage: GraphBridgesSpark <inputFilePath> <numVertices>");
            System.exit(1);
        }

        String inputFilePath = args[0];
        int numVertices = Integer.parseInt(args[1]);

        // Creating a SparkContext
        JavaSparkContext sc = new JavaSparkContext("local", "GraphBridgesSpark");

        // Starting the timer for graph creation
        long graphCreationStartTime = System.currentTimeMillis();

        // Reading the input file into a List
        List<String> inputLines = Collections.emptyList(); // Initialize with an empty list
        try {
            inputLines = Files.readAllLines(Paths.get(inputFilePath), StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Error reading the input file.");
            System.exit(1);
        }

        // Create an ArrayList to store edges instead of an RDD
        List<Tuple2<Integer, Integer>> edgeList = new ArrayList<>();
        for (String line : inputLines) {
            edgeList.addAll(parseEdge(line));
        }

        // Remove duplicates by considering only edges where srcVertex < destVertex
        List<Tuple2<Integer, Integer>> uniqueEdges = removeDuplicates(edgeList);

        // Create an ArrayList for the adjacency list instead of an RDD
        List<Tuple2<Integer, Iterable<Integer>>> adjacencyList = new ArrayList<>(sc.parallelizePairs(edgeList)
                .groupByKey()
                .collect());

        // Broadcast the adjacency list
        final Broadcast<List<Tuple2<Integer, Iterable<Integer>>>> broadcastAdjList =
                sc.broadcast(adjacencyList);
        
        // Stop the timer for graph creation
        long graphCreationEndTime = System.currentTimeMillis();
        long creationTime = graphCreationEndTime - graphCreationStartTime;

        // Starting the timer for computation
        long computationStartTime = System.currentTimeMillis();

        // Compute bridges and store the information in an ArrayList instead of an RDD
        List<Tuple2<Tuple2<Integer, Integer>, Boolean>> bridgesList = new ArrayList<>();
        for (Tuple2<Integer, Integer> edge : uniqueEdges) {
            bridgesList.addAll(computeBridge(edge, broadcastAdjList, sc, numVertices));
        }

        // Printing Results
        System.out.println("Graph Creation Elapsed time = " + (graphCreationEndTime - graphCreationStartTime) + " milliseconds");

        // Stop the timer for computation
        long computationEndTime = System.currentTimeMillis();
        System.out.println("Computation Elapsed time = " + (computationEndTime - computationStartTime) + " milliseconds");

        System.out.println("Number of bridges: " + bridgesList.size());

        // Filter and print the bridges
        System.out.println("Bridges:");
        bridgesList.stream()
        .filter(entry -> {
            boolean isBridge = entry._2();
            return isBridge;
        })  // Filter to print only bridges
        .forEach(entry -> System.out.println(entry._1()._1() + " and " + entry._1()._2()));

        

        // Stop the SparkContext
        sc.stop();
    }
    // To Remove Duplicate Edges
    private static List<Tuple2<Integer, Integer>> removeDuplicates(List<Tuple2<Integer, Integer>> edgeList) {
        Set<Tuple2<Integer, Integer>> uniqueEdgesSet = new HashSet<>();
        for (Tuple2<Integer, Integer> edge : edgeList) {
            int srcVertex = edge._1();
            int destVertex = edge._2();

            // Add the edge to the set only if src vertex is less than destVertex
            if (srcVertex < destVertex) {
                uniqueEdgesSet.add(edge);
            }
        }

        // Convert the set back to a list if needed
        return new ArrayList<>(uniqueEdgesSet);
    }

    // Extracted the logic for parsing an edge
    private static List<Tuple2<Integer, Integer>> parseEdge(String line) {
        String[] parts = line.split("=");
        int vertex = Integer.parseInt(parts[0]);
        String[] adjList = parts[1].split(";");
        Set<Tuple2<Integer, Integer>> uniqueEdges = new HashSet<>();

        for (String edge : adjList) {
            String[] edgeParts = edge.split(",");
            int adjVertex = Integer.parseInt(edgeParts[0]);
            Tuple2<Integer, Integer> currentEdge = new Tuple2<>(vertex, adjVertex);

            // Add the edge to the set to ensure uniqueness
            uniqueEdges.add(currentEdge);
        }

        // Convert the set to a list and return
        return new ArrayList<>(uniqueEdges);
    }

    private static List<Tuple2<Tuple2<Integer, Integer>, Boolean>> computeBridge(
        Tuple2<Integer, Integer> edge, Broadcast<List<Tuple2<Integer, Iterable<Integer>>>> adjacencyList,
        JavaSparkContext sc, int numVertices) {
        int srcVertex = edge._1();
        int destVertex = edge._2();

        // Removing the edge temporarily from the adjacency list
        List<Tuple2<Integer, Iterable<Integer>>> modifiedAdjacencyList = new ArrayList<>(adjacencyList.value());

        // Checking if the graph is connected using Breadth-First Search
        boolean isConnected = isGraphConnectedBFS(srcVertex, destVertex, numVertices, sc.broadcast(modifiedAdjacencyList));

        // Return the edge and its bridge status only if it is a bridge
        return isConnected ? Collections.emptyList() :
                Collections.singletonList(new Tuple2<>(new Tuple2<>(srcVertex, destVertex), true));
        }
        // Helper method to check if an Iterable contains a specific element
        private static <T> boolean contains(Iterable<T> iterable, T element) {
            for (T item : iterable) {
                if (item.equals(element)) {
                    return true;
                }
            }
            return false;
        }

    private static boolean isGraphConnectedBFS(int startVertex,int destVertex, int numVertices,
                                            Broadcast<List<Tuple2<Integer, Iterable<Integer>>>> adjacencyList) {
        Set<Integer> visited = new HashSet<>();
        Queue<Integer> frontier = new LinkedList<>();
        //Start BFS From 1st vertex
        int bfsStart = adjacencyList.value().get(0)._1();
        frontier.add(bfsStart);
        visited.add(bfsStart);

        while(!frontier.isEmpty())
            {
            int curr = frontier.poll();

            Iterable<Integer> neighbors = adjacencyList.value().stream()
                .filter(entry -> entry._1().equals(curr))
                .findFirst()
                .map(Tuple2::_2)
                .orElse(Collections.emptyList());

                for (int neighbor : neighbors) {
                    if(((! (curr == startVertex) || !(neighbor == destVertex)) &&
                    (!(curr == destVertex) || !(neighbor == startVertex)) &&
                    !visited.contains(neighbor))){
                        visited.add(neighbor);
                        frontier.add(neighbor);
                    }
                }
            }
            // Check if all vertices have been visited
            if (visited.size() == numVertices) {
                return true; // Graph is connected
            } else {
                return false; // Graph is disconnected
            }
        }
    
}

