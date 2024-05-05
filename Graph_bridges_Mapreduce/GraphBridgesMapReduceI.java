import java.io.IOException;
import java.util.*;
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

public class GraphBridgesMapReduce {

    public static class AdjacencyListMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        JobConf conf1;

    public void configure(JobConf job) {
        this.conf1 = job;
    }
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            String[] parts = value.toString().split("=");
            String node = parts[0];
            String[] neighborsWithWeights = parts[1].split(";");

            StringBuilder neighborsBuilder = new StringBuilder();
            for (String neighborWithWeight : neighborsWithWeights) {
                String[] neighborParts = neighborWithWeight.split(",");
                String neighbor = neighborParts[0];
                neighborsBuilder.append(neighbor).append(",");
            }

            // Remove the trailing comma if the neighbors list is not empty
            if (neighborsBuilder.length() > 0) {
                neighborsBuilder.setLength(neighborsBuilder.length() - 1);
            }

            output.collect(new Text(node), new Text(neighborsBuilder.toString()));
        }
    }

    public static class AdjacencyListReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            while (values.hasNext()) {
                output.collect(key, values.next());
            }
        }
    }

    public static class EdgeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        JobConf conf2;

    public void configure(JobConf job) {
        this.conf2 = job;
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        String[] parts = value.toString().split("\t");
        String node = parts[0];
        String[] neighbors = parts[1].split(",");

        HashSet<String> uniquePairs = new HashSet<>();

        for (String neighbor : neighbors) {
            if (!neighbor.equals(node)) {
                String edge = (Integer.parseInt(node) < Integer.parseInt(neighbor)) ? node + " " + neighbor : neighbor + " " + node;
                uniquePairs.add(edge);
            }
        }

        for (String pair : uniquePairs) {
            output.collect(new Text(""), new Text(pair));
        }
    }
}

public static class EdgeReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        HashSet<String> uniqueEdges = new HashSet<>();

        while (values.hasNext()) {
            String edge = values.next().toString();
            uniqueEdges.add(edge);
        }

        for (String edge : uniqueEdges) {
            output.collect(new Text(""), new Text(edge));
        }
    }
}

public static void main(String[] args) throws Exception {

        // Job 1: Create adjacency list
        JobConf conf1 = new JobConf(GraphBridgesMapReduce.class);
        conf1.setJobName("GraphBridgesMapReduce-AdjCreation");
        conf1.setMapperClass(AdjacencyListMapper.class);
        conf1.setReducerClass(AdjacencyListReducer.class);
        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf1, new Path(args[1] + "/Adjlist"));
        RunningJob job1 = JobClient.runJob(conf1);
        job1.waitForCompletion();

        // Job 2: Find bridges using the adjacency list
        JobConf conf2 = new JobConf(GraphBridgesMapReduce.class);
        conf2.setJobName("GraphBridgesMapReduce-EdgeCreation");

        conf2.setMapperClass(EdgeMapper.class);
        conf2.setReducerClass(EdgeReducer.class);

        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(conf2, new Path(args[1] + "/Adjlist"));
        FileOutputFormat.setOutputPath(conf2, new Path(args[1] + "/Edgelist"));

        // Run the second job and wait for its completion
        long time = System.currentTimeMillis();
        RunningJob job2 = JobClient.runJob(conf2);
        job2.waitForCompletion();
        System.out.println("Elapsed time = " + (System.currentTimeMillis() - time) + " ms");
    }
}
