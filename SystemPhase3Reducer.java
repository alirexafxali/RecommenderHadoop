package RecommendationSystem;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import scala.Tuple3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SystemPhase3Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        List<String> reducerValues = new ArrayList<>();
        for (Text value: values)
        {
            reducerValues.add(value.toString());
        }
        int groupSize = reducerValues.size(); //length of each vector
        int dotProduct = 0; // sum of ratingProd
        int rating1Sum = 0; // sum of rating1
        int rating2Sum = 0; // sum of rating2
        int rating1NormSq = 0; // sum of rating1Squared
        int rating2NormSq = 0; // sum of rating2Squared
        int maxNumOfumRaters1 = 0; // max of numOfRaters1
        int maxNumOfumRaters2 = 0; // max of numOfRaters2
        for (String mv : reducerValues)
        {

            dotProduct += Integer.parseInt(mv.split(",")[4].trim());
            rating1Sum += Integer.parseInt(mv.split(",")[0].replaceFirst("<","").trim());
            rating2Sum += Integer.parseInt(mv.split(",")[2].trim());
            rating1NormSq += Integer.parseInt(mv.split(",")[5].trim());
            rating2NormSq += Integer.parseInt(mv.split(",")[6].replaceAll(">","").trim());
            int numOfRaters1 = Integer.parseInt(mv.split(",")[1].trim());
            int numOfRaters2 = Integer.parseInt(mv.split(",")[3].trim());
            if (numOfRaters1 > maxNumOfumRaters1) {
                maxNumOfumRaters1 = numOfRaters1;
                }
            if (numOfRaters2 > maxNumOfumRaters2) {
                maxNumOfumRaters2 = numOfRaters2;
                }

        }
        double pearson = pearsonCorrelation.calculate(groupSize, dotProduct, rating1Sum,rating2Sum, rating1NormSq, rating2NormSq);
        context.write(key, new Text(String.valueOf(pearson)));
    }
}
