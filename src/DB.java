import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.Integer.parseInt;
import static oracle.jdbc.OracleTypes.NUMBER;
import static oracle.jdbc.OracleTypes.STRUCT;

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
        }
    }

    public void calculateSimilarity(){
        List<Long> filmsRecords = readValues();
        int distance = calcDistance();
        for(int i = 0 ; i < filmsRecords.size() ; i++){
            for (int j = (filmsRecords.size() - 1) ; j > i ;  j--){
                float similarity = calcSimilarity(filmsRecords.get(i), filmsRecords.get(j), distance);
                insertToSimilarityTable(filmsRecords.get(i), filmsRecords.get(j), similarity);
            }
        }
    }

    private void insertToSimilarityTable(Long mid1, Long mid2, float similarity) {
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement("INSERT INTO SIMILARITY (MID1, MID2, SIMILARITY) VALUES(?,?,?)");
            ps.setLong(1, mid1);
            ps.setLong(2, mid2);
            ps.setFloat(3, similarity);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private float calcSimilarity(Long mid1, Long mid2, int distance) {
        CallableStatement cs = null;
        try {
            cs = this.connection.prepareCall("{? =call SimCalculation(?,?,?)}");

            cs.registerOutParameter(1, Types.FLOAT);
            cs.setLong(2, mid1);
            cs.setLong(3, mid2);
            cs.setInt(4, distance);
            cs.execute();
            return cs.getFloat(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                cs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return -1;
    }


    private int calcDistance() {
        CallableStatement cs = null;
        try {
            cs = this.connection.prepareCall("{? =call MaximalDistance}");
            cs.registerOutParameter(1, Types.NUMERIC);
            cs.execute();
            return cs.getInt(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                cs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    private  List<Long> readValues() {
        List<Long> mediaItems = new ArrayList<Long>();
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement("SELECT MID FROM MEDIAITEMS");
            rs = ps.executeQuery();
            while ( rs.next() ) {
                Long mid = rs.getLong("MID");
                mediaItems.add(mid);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                rs.close();
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return mediaItems;
    }

    public void printSimilarItems(long mid) {
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement("select title, similarity from (select mediaitems.mid, mediaitems.title, tmp.similarity from mediaitems join (select similarity.mid2, similarity.mid1, similarity.similarity from similarity where (similarity.mid1 = ? or similarity.mid2 = ?) and similarity.similarity >= 0.3) tmp on mediaitems.mid != ? and (tmp.mid2 = mediaitems.mid or tmp.mid1 = mediaitems.mid))");
            ps.setLong(1,mid);
            ps.setLong(2,mid);
            ps.setLong(3,mid);
            rs = ps.executeQuery();
            while ( rs.next() ) {
                String title = rs.getString("Title");
                Float similarity = rs.getFloat("Similarity");
                System.out.println(title + " " + similarity);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                rs.close();
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
