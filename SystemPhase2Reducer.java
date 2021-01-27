package RecommendationSystem;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.shaded.com.google.inject.internal.cglib.proxy.$UndeclaredThrowableException;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.List;

public class SystemPhase2Reducer extends Reducer<LongWritable, Text, Text, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key,values,context);
        StringBuilder sb = new StringBuilder();
        for (Text value : values) {
            sb.append(",<"+value.toString()+">");
        }
        sb.append("@");
        context.write(new Text(key.toString()), new Text(sb.toString()));
    }
}
