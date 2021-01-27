package RecommendationSystem;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

public class SystemPhase1Cmbiner  extends Reducer<LongWritable, Text, Text, Text> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Integer numberOfRatings = 0;
        for(Text value:values)
        {
            numberOfRatings += 1;
        }
        for (Text value:values)
        {
            context.write(new Text(key.toString()), new Text(value+"\t"+numberOfRatings));
        }

}
}
