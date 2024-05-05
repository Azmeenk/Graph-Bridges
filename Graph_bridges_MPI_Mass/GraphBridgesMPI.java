import mpi.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class GraphBridgesMPI {
    public static void main(String[] args) throws MPIException, FileNotFoundException {
        // init
        MPI.Init(args);
        String inputFilePath = args[0];
        int numVertices = Integer.parseInt(args[1]);
        int my_rank = MPI.COMM_WORLD.Rank();
        int mpi_size = MPI.COMM_WORLD.Size();

        // start timer
        long startTime = System.currentTimeMillis();

        File inputFile = new File(inputFilePath);

        // split up work amongst the computing nodes
        int[] stripe = new int[mpi_size];
        int[] startIndex = new int[mpi_size];
        splitUpWork(stripe, startIndex, numVertices);

        // set up partial adjacency list
        int[] adjList = new int[numVertices * numVertices];
        int[] numEdges = new int[2]; // 0th index = num edges found by computing node, 1st index = total num edges
        ArrayList<Integer> srcList = new ArrayList<>();
        ArrayList<Integer> destList = new ArrayList<>();

        int endIndex = startIndex[my_rank] + stripe[my_rank] - 1;
        numEdges[0] = setupAdjList(startIndex[my_rank], endIndex, adjList, numVertices, srcList, destList, inputFile);

        // broadcast partial adjacency list to all other computing nodes
        for (int i = 0; i < mpi_size; i++) {
            MPI.COMM_WORLD.Bcast(adjList, startIndex[i] * numVertices, stripe[i] * numVertices, MPI.INT, i);
        }

        // Use all reduce to add up the number of edges in all computing nodes
        MPI.COMM_WORLD.Allreduce(numEdges, 0, numEdges, 1,  1, MPI.INT, MPI.SUM);

        // set up edges list
        int[] edgesSrc = new int[numEdges[1]];
        int[] edgesDest = new int[numEdges[1]];
        int[] isBridge = new int[numEdges[1]];

        if (my_rank == 0) {
            // add master computing node's edges into complete list
            int index = 0;
            for (; index < numEdges[0]; index++) {
                edgesSrc[index] = srcList.get(index);
                edgesDest[index] = destList.get(index);
            }

            // recv edges from each computing node and add to complete list
            for (int i = 1; i < mpi_size; i++) {
                // get number of edges found by computing node
                int[] tempNumEdges = new int[1];
                MPI.COMM_WORLD.Recv(tempNumEdges, 0, 1, MPI.INT, i, 0);

                // get edges list from computing node
                MPI.COMM_WORLD.Recv(edgesSrc, index, tempNumEdges[0], MPI.INT, i, 0);
                MPI.COMM_WORLD.Recv(edgesDest, index, tempNumEdges[0], MPI.INT, i, 0);

                // move index to next available space in array
                index += tempNumEdges[0];
            }
        }
        else {
            // Send number of edges found to master computing node
            MPI.COMM_WORLD.Send(numEdges, 0, 1, MPI.INT, 0, 0);

            int[] srcArray = srcList.stream().mapToInt(i -> i).toArray();
            int[] destArray = destList.stream().mapToInt(i -> i).toArray();

            // Send edges to master computing node
            MPI.COMM_WORLD.Send(srcArray, 0, numEdges[0], MPI.INT, 0, 0);
            MPI.COMM_WORLD.Send(destArray, 0, numEdges[0], MPI.INT, 0, 0);
        }

        // stop timer and print elapsed time
        long endTime = System.currentTimeMillis();
        if (my_rank == 0) {
            System.out.println( "Graph Creation Elapsed Time = " + (endTime - startTime) + " milliseconds");
            System.out.println();
        }

        // start timer
        startTime = System.currentTimeMillis();

        // split up edges amongst the computing nodes
        splitUpWork(stripe, startIndex, numEdges[1]);
        MPI.COMM_WORLD.Scatterv(edgesSrc, 0, stripe, startIndex, MPI.INT, edgesSrc, startIndex[my_rank], stripe[my_rank], MPI.INT, 0);
        MPI.COMM_WORLD.Scatterv(edgesDest, 0, stripe, startIndex, MPI.INT, edgesDest, startIndex[my_rank], stripe[my_rank], MPI.INT, 0);

        // each computing node finds bridges from assigned set of edges
        endIndex = startIndex[my_rank] + stripe[my_rank] - 1;
        findBridges(startIndex[my_rank], endIndex, adjList, numVertices, edgesSrc, edgesDest, isBridge);

        // all nodes send their results to master node
        MPI.COMM_WORLD.Gatherv(isBridge, startIndex[my_rank], stripe[my_rank], MPI.INT, isBridge, 0, stripe, startIndex, MPI.INT, 0);

        // stop timer
        endTime = System.currentTimeMillis();

        // print results
        if (my_rank == 0) {
            // print bridge edges
            System.out.println("Bridges: ");
            for (int i = 0; i < isBridge.length; i++) {
                if (isBridge[i] == 1) {
                    System.out.println(edgesSrc[i] + " and " + edgesDest[i]);
                }
            }
            System.out.println();

            // print elapsed time
            System.out.println( "Computation Elapsed Time = " + (endTime - startTime) + " milliseconds");
        }

        MPI.Finalize();
    }

    /**
     * Sets up the adjacency list based on input file
     */
    private static int setupAdjList(int startIndex, int endIndex, int[] adjList, int numVertices, ArrayList<Integer> srcList, ArrayList<Integer> destList, File inputFile) throws FileNotFoundException {
        int numEdges = 0; // keep track of number of edges created

        Scanner fileReader = new Scanner(inputFile);

        // skip all the lines before start index
        for (int i = 0; i < startIndex; i++) {
            if (fileReader.hasNextLine()) {
                fileReader.nextLine();
            }
        }

        // process assigned set of lines
        for (int i = startIndex; i <= endIndex; i++) {
            if (fileReader.hasNextLine()) {
                // get next line and tokenize it
                String line = fileReader.nextLine();
                StringTokenizer lineTokenizer = new StringTokenizer(line, "=");

                // get vertex
                int vertex = Integer.parseInt(lineTokenizer.nextToken());

                // process all adjacent vertices
                StringTokenizer adkTokenizer = new StringTokenizer(lineTokenizer.nextToken(), ";");
                while (adkTokenizer.hasMoreTokens()) {
                    String edge = adkTokenizer.nextToken();
                    StringTokenizer edgeTokenizer = new StringTokenizer(edge, ",");

                    // get adjacent vertex
                    int adjVertex = Integer.parseInt(edgeTokenizer.nextToken());

                    // add adjacent vertex to adjacency list
                    adjList[vertex * numVertices + adjVertex] = 1;

                    // add edge to src and dest lists only if adjacent vertex is less than vertex numerically
                    // this prevents the storage of duplicate edges in a undirected graph
                    if (adjVertex > vertex) {
                        srcList.add(vertex);
                        destList.add(adjVertex);
                        numEdges++;
                    }
                }
            }
        }

        fileReader.close();

        return numEdges;
    }

    /**
     * Determines which of the assigned edges are bridges by removing edge from graph and running bfs
     */
    private static void findBridges(int startIndex, int endIndex, int[] adjList, int numVertices, int[] edgesSrc, int[] edgesDest, int[] isBridge) {
        for (int i = startIndex; i <= endIndex; i++) {
            int srcVertex = edgesSrc[i];
            int destVertex = edgesDest[i];

            // remove edge temporarily
            adjList[srcVertex * numVertices + destVertex] = 0;
            adjList[destVertex * numVertices + srcVertex] = 0;

            // check if graph is not connected after edge removal
            if (!isGraphConnectedBFS(adjList, numVertices)) {
                isBridge[i] = 1;
            }

            // add edge back
            adjList[srcVertex * numVertices + destVertex] = 1;
            adjList[destVertex * numVertices + srcVertex] = 1;
        }
    }

    /**
     * Uses bfs to determine if graph is connected
     */
    private static boolean isGraphConnectedBFS(int[] adjList, int numVertices) {
        HashSet<Integer> visited = new HashSet<>();
        Queue<Integer> frontier = new LinkedList<>();
        frontier.add(0);
        while (!frontier.isEmpty()) {
            // get current vertex and set it as visited
            int curr = frontier.poll();

            // if current vertex is visited, then skip it
            if (visited.contains(curr)) {
                continue;
            }

            // add current vertex to visited
            visited.add(curr);

            // for all neighboring vertices
            for (int i = 0; i < numVertices; i++) {
                // add adjacent vertices to queue
                if (adjList[curr * numVertices + i] == 1) {
                    frontier.add(i);
                }
            }
        }

        // if all vertices visted, then return true: graph is connected
        return visited.size() == numVertices;
    }

    /**
     * split up work between the computing nodes
     */
    private static void splitUpWork(int[] stripe, int[] startIndex, int size)
    {
        int mpi_size = stripe.length;

        // split up work evenly
        int baseStripe = size / mpi_size;
        int remainder = size % mpi_size;

        // handle remainder
        for (int i = 0; i < mpi_size; i++)
        {
            stripe[i] = (i < remainder) ? baseStripe + 1 : baseStripe;
            startIndex[i] = (i < remainder) ? (baseStripe * i + i) : baseStripe * i + remainder;
        }
    }
}