package RecommendationSystem;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;

public class SystemPhase1Mapper extends Mapper<Object, Text, LongWritable, Text> {

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] tokens = line.split("\t");
        String user = tokens[0];
        Long movie = Long.parseLong(tokens[1]);
        String rating = tokens[2];
        //System.out.println(movie.toString()+","+user.toString()+","+rating.toString());
        context.write(new LongWritable(movie),new Text(user+ "\t"+ rating));

    }

}
