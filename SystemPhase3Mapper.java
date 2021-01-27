package RecommendationSystem;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class SystemPhase3Mapper extends Mapper<Object, Text, Text, Text> {
    protected void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String[] users = value.toString().split("@");
        //int cnt =0;
        for (String user: users)
        {
            //if (cnt ==5)
            //    break;

            String[] movieRatings = user.split(",");
            String UserId = movieRatings[0].trim();
            //System.out.println(UserId);
            for (int i=1; i<movieRatings.length;i++)
            {
            String[] movieRating1 = movieRatings[i].split("\t");
            //Movie
                String movie1= movieRating1[0].replaceAll("<","").trim();
            //NumberOfRatings
                String numOfRating1 = movieRating1[2].replaceAll(">","").trim();
            //length
            for (int j=i+1;j<movieRatings.length;j++)
            {
                String[] movieRating2 = movieRatings[j].split("\t");
                //Movie
                String movie2 = movieRating2[0].replaceAll("<","").trim();
                //NumberOfRating
                String numOfRating2 = movieRating2[2].replaceAll(">","").trim();
                String moviesCombined = "<"+movie1+","+movie2+">";
                float ratingsProduct = Float.parseFloat(movieRating1[1].trim())*Float.parseFloat(movieRating2[1].trim());
                float rating1Squared = Float.parseFloat(movieRating1[1].trim())*Float.parseFloat(movieRating1[1].trim());
                float rating2Squared = Float.parseFloat(movieRating2[1].trim())*Float.parseFloat(movieRating2[1].trim());

                String mapperValue = "<"+movieRating1[1]+","+numOfRating1+","+movieRating2[1]+","+numOfRating2+","+ratingsProduct+","+rating1Squared+","+rating2Squared+">";
                //System.out.println(moviesCombined);
                //System.out.println(mapperValue);
                //System.out.println(moviesCombined+"\t"+mapperValue);
                context.write(new Text(moviesCombined), new Text(mapperValue));
                movieRating2 = null;
            }
            movieRating1 = null;
            }
            movieRatings = null;
            //cnt++;
            //System.gc();
        }
    }
}
