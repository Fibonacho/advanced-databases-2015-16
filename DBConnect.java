import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

public class DBConnect {

    Connection con = null;
    Statement st   = null;
    ResultSet rs   = null;
    ArrayList<Integer> numbOfSecureSeatRestarts = new ArrayList<Integer>();
    ArrayList<Integer> numbOfAvailabilityRestarts = new ArrayList<Integer>();

    /**
     * constructor
     */
    public DBConnect() {

        String host = "localhost"; // for local use
        String pwd  = "abc";
        String user = "testuser";
        
        // "build" url out of user, pwd and host
        String url = "jdbc:oracle:thin:" + user + "/" + pwd + "@" + host;

        try {
            con = DriverManager.getConnection(url, user, pwd);
            st = con.createStatement();
            System.err.println("Connection established.");
        } catch (Exception ex) {
            System.err.println("Could not establish connection.");
            ex.printStackTrace();
            return;
        }
    }

    /**
     * print numbOfSecureSeatRestarts
     */
    public void printNumbOfSecureSeatRestarts() {        
        for (int i = 0; i < numbOfSecureSeatRestarts.size(); i++) {
            System.out.print(numbOfSecureSeatRestarts.get(i) + "; ");
        }
        System.out.println("\nSecure seat tries:  " + numbOfSecureSeatRestarts.size());
    }
    
    /**
     * print numbOfRestarts
     */
    public void printNumbOfAvailabilityRestarts() {        
        for (int i = 0; i < numbOfAvailabilityRestarts.size(); i++) {
            System.out.print(numbOfAvailabilityRestarts.get(i) + "; ");
        }
        System.out.println("\nAvailability tries: " + numbOfAvailabilityRestarts.size());
    }
    
    /**
     * select * from table
     */
    public void selectAllData(String table) throws SQLException {
        
        rs = st.executeQuery("SELECT * FROM " + table);
        
        while(rs.next()) {
            System.out.print(rs.getInt(1) + "\t");
            System.out.println(rs.getString(2));
        }
    }
    
    /**
     * drop whole table (content and scheme)
     */
    public void dropTable() {
        
        String deleteTableSQL = "DROP TABLE flight_seats";
        
        try {
            st.executeUpdate(deleteTableSQL);
        } catch (SQLException ex) {
            System.err.println("Could not drop table 'flight_seats'.");
            ex.printStackTrace();
        }
    }
    
    /** from assignment:
     *  We imitate flight seat reservation procedure. The seats availability is stored in the following table:
     *      flight_seats[id, availability]
     *      - There are 200 seats; Id is a primary key with integer values from 1 to 200.
     *      - Availability is a boolean value indicating if the seat is available or not (initially true).
     */
    public void createTableScheme() {
        
        String createTableSQL = "CREATE TABLE flight_seats("
                                + "id integer not null check (id > 0 and id <= 200), "
                                + "availability integer check (availability in (0,1)), "
                                + "primary key (id)"
                                + ")";

        try {
            st.executeUpdate(createTableSQL);
        } catch (SQLException ex) {
            System.err.println("Could not create table 'flight_seats'.");
            ex.printStackTrace();
        }        
    }
    
    /**
     * put data into table 'flight_seats' (ids 1-200, availability true (1) initially)
     */
    public void fillTable() {
        
        for (int i = 1; i <= 200; i++) {
            try {
                String qry = "INSERT INTO flight_seats VALUES (" + i + "," + "1)";
                st.executeUpdate(qry);
            } catch (SQLException ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
    }
    
    /**
     * retrieve a list (of available seats)
     */
    public ArrayList<Integer> getList() {
        
        ArrayList<Integer> result = new ArrayList<Integer>();
        
        try {
            rs = st.executeQuery("SELECT id FROM flight_seats WHERE availability = 1");            
            if (rs != null) {
                while (rs.next()) {
                    result.add(rs.getInt(1));
                } 
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
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
     * The booking is done within 3 steps - this method is used for testing (in Test.java)
     */
    public void bookSeat() {
        
        // 1. Retrieve list of available seats (selection of available seat ids). -> i.e. rows where availability = 1
        ArrayList<Integer> availSeats = this.getList();
        System.out.println("number of available seats: " + availSeats.size());
        
        // 2. Give the customer some time (decision time is 1 second) to decide on a seat (a random seat id from the list returned in point 1).
        Random r        = new Random();
        int low         = 0;                 // lower bound for random index
        int high        = availSeats.size(); // upper bound for random index
        int randomIndex = r.nextInt(high-low) + low; // compute random index
        
        Integer randomSeat = availSeats.get(randomIndex); // select (random) seat number
        System.out.println("SEAT: " + randomSeat);
        
        // 3. Secure a seat (update the availability of the chosen seat to false).
        String secureSeat = "UPDATE flight_seats SET availability = 0 WHERE id = " + randomSeat;
        boolean ok = this.updateData(secureSeat);
        System.out.println("booked? " + ok);
    }    
    
    /**
     * set isolation level and send query to database
     * mode: 0 - read committed, 1 - serializable
     */
    public void setIsolationLevelAndSecureSeat(String query, int mode, int qryID) {
        
        int level = -1;
        String isolationLevel = "";
        if (mode == 0) {
            isolationLevel = "read committed";
            level = Connection.TRANSACTION_READ_COMMITTED;
        } else if (mode == 1) {
            isolationLevel = "serializable";
            level = Connection.TRANSACTION_SERIALIZABLE;           
        }
        //System.out.println("Query " + qryID + ": Secure seat with " + isolationLevel);
        
        try {
            con.setTransactionIsolation(level);
        } catch (SQLException ex) {
            System.err.println("Setting transaction isolation level failed.");
            ex.printStackTrace();
        }
        
        int cur;
        try {
            cur = con.getTransactionIsolation();
            System.out.println("Level: " + cur);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        
        boolean ok = false;
        int loops = 0; // count tries per transaction (customer)
        while (!ok) {
            loops++;
            try {
                System.out.println("QueryID: " + qryID + "; SECURE SEAT with " + isolationLevel + "; TRY: " + loops);
                st.execute(query);
            } catch (SQLException ex1) {
                System.err.println("Query could not be executed; rollback and try again.");
                ex1.printStackTrace();
    
                try {
                    con.rollback();
                } catch (SQLException ex2) {
                    System.err.println("Could not rollback.");
                    ex2.printStackTrace();
                }
                continue;
            }
            ok = true;
            numbOfSecureSeatRestarts.add(loops);
        }
    }
  
    /**
     * set isolation level, send query to database and retrieve available seats
     * mode: 0 - read committed, 1 - serializable
     */
    public ArrayList<Integer> setIsolationLevelAndRetrieveSeats(String query, int mode, int qryID) {
    
        ArrayList<Integer> result = new ArrayList<Integer>();
        
        int level = -1;
        String isolationLevel = "";
        if (mode == 0) {
            isolationLevel = "read committed";
            level = Connection.TRANSACTION_READ_COMMITTED;
        } else if (mode == 1) {
            isolationLevel = "serializable";
            level = Connection.TRANSACTION_SERIALIZABLE;
        }
        //System.out.println("Query " + qryID + ": Retrieve availability with " + isolationLevel);
        
        try {
            con.setTransactionIsolation(level);
        } catch (SQLException e) {
            System.err.println("Setting transaction isolation level failed (" + isolationLevel + ").");
            e.printStackTrace();
        }
        
        try {
            int cur = con.getTransactionIsolation();
            //System.out.println("Level: " + cur);
        } catch (SQLException e) {
            System.err.println("getTransactionIsolation() FAILED!");
            e.printStackTrace();
        }
        
        boolean ok = false;
        int loops = 0;
        while (!ok) {
            loops++;
            try {
                System.out.println("QueryID: " + qryID + "; RETRIEVE AVAILABILITY with " + isolationLevel + "; TRY: " + loops);
                rs = st.executeQuery(query);                
                if (rs != null && rs.next()) {
                    do {
                        result.add(rs.getInt(1));
                    } while (rs != null && rs.next());
                }                
                if (result.size() <= 200) {
                    // must not be greater than 200
                    ok = true;
                } else {
                    // try again
                    result = null;
                    continue;
                }
            } catch (SQLException ex) {
                System.err.println("Query could not be executed, rollback and try again.");
                ex.printStackTrace();
                try {
                    con.rollback();
                } catch (SQLException ex1) {
                    ex1.printStackTrace();
                }
                continue;
            }
            numbOfAvailabilityRestarts.add(loops);
        }
        return result;
    }
}
