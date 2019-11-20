import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.Integer.parseInt;

public class DB {
    private String userName;
    private String psw;
    private Connection connection;

    public DB(String connectionString, String userName, String psw){
            this.userName = userName;
            this.psw = psw;
            try {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                this.connection = DriverManager.getConnection(connectionString, userName, psw);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }

    public void fileToDataBase(String path) throws SQLException {

        String line = "";
        String cvsSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {

            while ((line = br.readLine()) != null) {

                String[] films = line.split(cvsSplitBy);

                PreparedStatement ps = this.connection.prepareStatement("INSERT INTO MediaItems (TITLE, PROD_YEAR) VALUES(?,?)");
                ps.setString(1,films[0]);
                ps.setInt(2,parseInt(films[1]));
                ps.executeUpdate();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
//            if ( this.connection != null)
//                this.connection.close();
        }


    }
}
