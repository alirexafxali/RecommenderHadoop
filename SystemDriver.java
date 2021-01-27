package RecommendationSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SystemDriver  extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "RecommenderPhase1");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: <input> <output>");
            System.exit(1);
        }

        job1.setJarByClass(SystemDriver.class);
        job1.setJarByClass(SystemPhase1Mapper.class);
        job1.setJarByClass(SystemPhase1Reducer.class);
        job1.setMapperClass(SystemPhase1Mapper.class);
        //job1.setCombinerClass(SystemPhase1Cmbiner.class);
        job1.setReducerClass(SystemPhase1Reducer.class);
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputPath);
        if (!job1.waitForCompletion(true))
        {
            System.out.println("Job 1 finished with errors!");
            return 0;
        }

        Job job2 = Job.getInstance(conf, "RecommenderPhase2");
        job2.setJarByClass(SystemDriver.class);
        job2.setJarByClass(SystemPhase2Mapper.class);
        job2.setJarByClass(SystemPhase2Reducer.class);
        job2.setMapperClass(SystemPhase2Mapper.class);
        job2.setReducerClass(SystemPhase2Reducer.class);
        FileInputFormat.addInputPath(job2, outputPath);
        Path phase2OutPath = new Path(args[1] + "_phase2Out");
        FileOutputFormat.setOutputPath(job2, phase2OutPath);
        if (!job2.waitForCompletion(true))
        {
            System.out.println("Job 2 finished with errors!");
            return 0;
        }
        Job job3 = Job.getInstance(conf, "RecommenderPhase3");
        job3.setJarByClass(SystemDriver.class);
        job3.setJarByClass(SystemPhase3Mapper.class);
        job3.setJarByClass(SystemPhase3Reducer.class);
        job3.setMapperClass(SystemPhase3Mapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setReducerClass(SystemPhase3Reducer.class);
        //job3.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.setInputPaths(job3, phase2OutPath);
        Path phase3OutPath = new Path(args[1] + "_phase3Out");
        FileOutputFormat.setOutputPath(job3, phase3OutPath);

        if (!job3.waitForCompletion(true))
        {
            System.out.println("Job 3 finished with errors!");
            return 0;
        }
        return 1;
    }
    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new SystemDriver(), args);
        System.exit(exitCode);
    }
}


