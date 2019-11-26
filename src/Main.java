public class Main {

    public static void main(String[] args){
        Assigment db = new Assigment("jdbc:oracle:thin:@132.72.65.216:1521/ORACLE", "saarzeev", "abcd");
        db.fileToDataBase("src/films.csv");
        db.calculateSimilarity();
        db.printSimilarItems(32);
    }
}
