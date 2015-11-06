import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

public class DBConnect {

    Connection con = null;
    Statement st = null;
    ResultSet rs = null;

    /**
     * constructor
     */
    public DBConnect() {

        String host = "localhost"; // for local use only!
        String pwd  = "abc";
        String user = "testuser";
        
        // "build" url out of user, pwd and host
        String url = "jdbc:oracle:thin:" + user + "/" + pwd + "@" + host;

        try {
            con = DriverManager.getConnection(url, user, pwd);
            st = con.createStatement();
            System.err.println("Connection established.");
        } catch (Exception e) {
            System.err.println("Could not establish connection.");
            e.printStackTrace();
            return;
        }
    }

    /**
     * select * from tableName
     */
    public void selectAllData(String tableName) throws SQLException {
        
        rs = st.executeQuery("SELECT * FROM " + tableName);
        while(rs.next()) {
            System.out.print(rs.getInt(1) + "\t");
            System.out.println(rs.getString(2));
        }
    }
    
    /**
     * drop table content and scheme
     */
    public void dropTable(String table) {
        
        String deleteTableSQL = "drop table " + table;
        try {
            st.executeUpdate(deleteTableSQL);
        } catch (SQLException e) {
            System.err.println("Could not drop table '" + table + "'.");
            e.printStackTrace();
        }
    }
    
    /** from assignment:
     *  We imitate flight seat reservation procedure. The seats availability is stored in the following table:
     *      flight_seats[id, availability]
     *      - There are 200 seats.
     *      - Id is a primary key with integer values from 1 to 200.
     *      - Availability is a boolean value indicating if the seat is available or not (initially true).
     */
    public void createTableScheme() {
        
        String createTableSQL = "create table flight_seats("
                                + "id integer not null check (id > 0 and id <= 200), "
                                + "availability char check (availability in (0,1)), "
                                + "primary key (id)"
                                + ")"
                                ;
        
        try {
            st.executeUpdate(createTableSQL);
        } catch (SQLException e) {
            System.err.println("Could not create table 'flight_seats'.");
            e.printStackTrace();
        }        
    }
    
    /**
     * put data into table 'flight_seats' (ids 1-200, availability true (1) initially)
     */
    public void fillTable() {
        
        for (int i = 1; i <= 200; i++) {
            try {
                String qry = "insert into flight_seats values (" + i + "," + "1)";
                st.executeUpdate(qry);
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
    }
    
    /**
     * retrieve a list (of available seats)
     */
    public ArrayList<String> getList(String qry) {
        
        ArrayList<String> result = new ArrayList<String>();
        try {
            rs = st.executeQuery(qry);
            
            if (rs.next()) {
                do {
                    result.add(rs.getString(1));
                } while (rs.next());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }        
        return result;
    }
    
    /**
     * update data (set availability to false - 0)
     */
    public boolean updateData(String qry) {
        
        try {
            rs = st.executeQuery(qry); 
            if (rs.next())
                return true;
        } catch (SQLException e) {
            System.err.println("Error in executeQuery()");
            e.printStackTrace();
        }
        return false;
    }
    
    /**
     * execute query and return result
     */
    public String executeQuery(String qry) {
        
        String result = "";        
        try {
            rs = st.executeQuery(qry);
            if (rs.next())
                result = rs.getString(1);            
        } catch (SQLException e) {
            System.err.println("Error in executeQuery()");
            e.printStackTrace();
        }        
        return result;
    }
    
    /** 
     *  The booking is done within 3 steps
     */
    public void bookSeat(String tableName) {
        
        // 1. Retrieve list of available seats (selection of available seat ids). -> i.e. rows where availability = 1
        String query = "select id from " + tableName + " where availability = 1";
        ArrayList<String> availSeats = this.getList(query);
        System.out.println("number of available seats: " + availSeats.size());
        
        // 2. Give the customer some time (decision time is 1 second) to decide on a seat (a random seat id from the list returned in point 1).
        Random r        = new Random();
        int low         = 0;                 // lower bound for random index
        int high        = availSeats.size(); // upper bound for random index
        int randomIndex = r.nextInt(high-low) + low; // compute random index
        
        String randomSeat = availSeats.get(randomIndex); // select (random) seat number
        System.out.println("SEAT: " + randomSeat);
        
        // 3. Secure a seat (update the availability of the chosen seat to false).
        String secureSeat = "update " + tableName + " set availability = 0 where id = " + randomSeat;
        boolean ok = this.updateData(secureSeat);
        System.out.println("booked? " + ok);
    }    
    
    /**
     * send query to database (read committed)
     */
    public void sendReadCommitted(String query) {
        
        System.out.println("read committed");
        int level = Connection.TRANSACTION_READ_COMMITTED;
        try {
            con.setTransactionIsolation(level);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        int cur;
        try {
            cur = con.getTransactionIsolation();
            System.out.println("Level: " + cur);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            st.execute(query);
        } catch (SQLException e) {
            System.err.println("Query could not be executed (read committed).");
            e.printStackTrace();
        }
    }
    
    /**
     * send query to database (serializable)
     */
    public void sendSerializable(String query) {
        
        System.out.println("serializable");
        int level = Connection.TRANSACTION_SERIALIZABLE;
        try {
            con.setTransactionIsolation(level);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        int cur;
        try {
            cur = con.getTransactionIsolation();
            System.out.println("Level: " + cur);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        try {
            st.execute(query);
        } catch (SQLException e) {
            System.err.println("Query could not be executed (serializable).");
            e.printStackTrace();
        }
    }
}
