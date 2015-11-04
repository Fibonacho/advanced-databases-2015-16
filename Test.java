import java.sql.SQLException;

/**
 * test program to imitate a flight seat reservation procedure 
 */
public class Test {

    public static void main(String[] args) {
        
        // setup jdbc driver
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            System.err.println("Driver found.");
        } catch (ClassNotFoundException e) {
            System.err.println("Oracle JDBC Driver not found ... ");
            e.printStackTrace();
            return;
        }

        // connect to database
        DBConnect db = new DBConnect();
        
        // procedure
        String tableName = "flight_seats";       
        db.dropTable(tableName); // delete table content and scheme 
        db.createTableScheme();  // create table scheme        
        db.fillTable();          // fill table with data (id 1-200, availability true (=1) initially)
        db.bookSeat(tableName);  // book a random seat
        
        // select * from table  
        try {
            db.selectAllData(tableName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
