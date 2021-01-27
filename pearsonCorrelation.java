package RecommendationSystem;

public class pearsonCorrelation {
    static double calculate(double size, double dotProduct, double rating1Sum, double rating2Sum, double rating1NormSq, double rating2NormSq) {

         double numerator = size * dotProduct - rating1Sum * rating2Sum;
         double denominator = Math.sqrt(size * rating1NormSq - rating1Sum * rating1Sum) * Math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum);
         return numerator / denominator;

    }
}
