package edu.uwb.css534;

import edu.uw.bothell.css.dsl.MASS.*;
import edu.uw.bothell.css.dsl.MASS.graph.transport.*;
import edu.uw.bothell.css.dsl.MASS.logging.LogLevel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GraphBridgesMass {
    private static final String NODE_FILE = "nodes.xml";

    public static void main( String[] args ) throws IOException {
        String inputFilePath = args[0];
        int numVertices = Integer.parseInt(args[1]);

        // init MASS library
        MASS.setNodeFilePath(NODE_FILE);
        MASS.setLoggingLevel(LogLevel.DEBUG);
        MASS.init();

        // start timer
        long startTime = System.currentTimeMillis();

        // create graph based on dsl input file
        GraphPlaces graph = new GraphPlaces(0, VertexPlace.class.getName());
        graph.loadDSLFile(inputFilePath);

        // get edge list
        ArrayList<edu.uwb.css534.Edge> edges = new ArrayList<>();
        for (int vertexId = 0; vertexId < numVertices; vertexId++) {
            VertexPlace vertex = graph.getVertex(vertexId);

            for (int j = 0; j < vertex.neighbors.size(); j++) {
                int neighbor = (int) vertex.neighbors.get(j);

                // add edge to list
                if (vertexId < neighbor) { // conditional to avoid duplicate edges
                    edges.add(new edu.uwb.css534.Edge(vertexId, neighbor));
                }
            }
        }

        // create places out of edge list
        Places edgeList = new Places(0, edu.uwb.css534.Edge.class.getName(), null, edges.size());
        edgeList.callAll(edu.uwb.css534.Edge.init_, edges);

        // end timer
        long endTime = System.currentTimeMillis();

        // print elapsed time
        MASS.getLogger().debug( "Graph Creation Elapsed Time = " + (endTime - startTime) + " milliseconds");
        MASS.getLogger().debug( "");

        // start timer
        startTime = System.currentTimeMillis();

        // perform bfs for each edge
        edgeList.callAll(edu.uwb.css534.Edge.isConnectedBFS_, graph);

        // stop timer
        endTime = System.currentTimeMillis();

        // Print bridges
        MASS.getLogger().debug( "Bridges List: ");
        edgeList.callAll(edu.uwb.css534.Edge.printBridge_);
        MASS.getLogger().debug( "");

        // print elapsed time
        MASS.getLogger().debug( "Computation Elapsed Time = " + (endTime - startTime) + " milliseconds");

        MASS.finish();
    }
}