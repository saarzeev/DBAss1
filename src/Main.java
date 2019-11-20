import java.sql.SQLException;

public class Main {

    public static void main(String[] args){
        DB db = new DB("jdbc:oracle:thin:@132.72.65.216:1521/ORACLE", "kovalkov", "abcd");
        try {
            db.fileToDataBase("src/films.csv");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
