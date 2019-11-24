import javafx.util.Pair;

import java.sql.SQLException;
import java.util.List;

public class Main {

    public static void main(String[] args){
        DB db = new DB("jdbc:oracle:thin:@132.72.65.216:1521/ORACLE", "kovalkov", "abcd");
        //            db.fileToDataBase("src/films.csv");
        db.printSimilarItems(32);
        int i = 5;
    }
}
