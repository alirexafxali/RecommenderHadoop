package RecommendationSystem;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import scala.Tuple2;

import java.io.IOException;

public class SystemPhase2Mapper extends Mapper<Object, Text, LongWritable, Text> {


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split("\t");
        LongWritable User = new LongWritable(Long.parseLong(tokens[0]));
        String movie = tokens[1];
        String rating = tokens[2];
        String numberOfRatings = tokens[3];
        context.write(User, new Text(movie+"\t"+rating+"\t"+numberOfRatings));
    }

}
