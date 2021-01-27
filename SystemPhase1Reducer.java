package RecommendationSystem;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class SystemPhase1Reducer extends Reducer<LongWritable, Text, Text, Text> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        HashSet<String> valuesList = new HashSet<String>();
        while(values.iterator().hasNext())
        {
            valuesList.add(values.iterator().next().toString());
        }
        int numberOfRatings = valuesList.size();

        //System.out.println(numberOfRatings);
        Text t3 = new Text();
        Text k3 = new Text();
        for(String t2: valuesList)
        {
            String value = t2;
            String[] vals = value.split("\t");
            k3 = new Text(vals[0]);
            //System.out.println(k3);
            //Tuple3<Text,IntWritable,IntWritable> v3 = new Tuple3(key, t2._2, numberOfRaters);
            //System.out.println(new Text(vals[0]+","+key+","+vals[1]+","+numberOfRatings));
            t3 = new Text(key+"\t"+vals[1]+"\t"+numberOfRatings);
            context.write(k3, t3);
        }
    }

}