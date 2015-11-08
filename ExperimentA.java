import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// BookingTransaction
class BookingTransaction extends Thread {

    // identifier of the transaction
    int id, mode, qry;
    Connection conn;
    ResultSet  rs;
    public int[]     tries   = new int[200];
    public boolean[] booking = new boolean[200];

    // constructor
    BookingTransaction(int id, Connection conn, int qry, int mode) {        
        this.id   = id;
        this.conn = conn;
        this.mode = mode;
        this.qry  = qry;
    }

    @Override
    public void run() {        
        int decisionTime = 1000;
        int randomSeat   = 0;
        long startTime   = System.currentTimeMillis();
        ArrayList<Integer> availSeats = new ArrayList<Integer>();

        // 1. Retrieve list of available seats
        try {
            conn.setAutoCommit(false);
            String query = "SELECT id FROM flight_seats WHERE availability = 1";
            rs = conn.createStatement().executeQuery(query);
            
            // iterate through result set 
            if (rs != null && rs.next()) {
                do {
                    availSeats.add(rs.getInt(1));
                } while (rs.next());
            }
        } catch (SQLException e) {
            System.err.println("transaction " + id + " failed while retrieving list of available seats.");
            e.printStackTrace();
        } finally {
            try {
                rs.close();
            } catch (SQLException e) {
                System.err.println("Could not close ResultSet.");
                e.printStackTrace();
            }
        }
        
        // 2. Give the customer some time (decision time is 1 second) to decide on a seat
        try {
            try {
                sleep(decisionTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
                currentThread().interrupt();
            }            
            randomSeat = getRandomSeatNumber(availSeats);
        } catch (Exception e) {
            System.err.println("transaction " + id + " failed while retrieving list of available seats.");
        }
        
        // 3. Secure a seat (update the availability of the chosen seat to false - 0).
        try {
            conn.createStatement().execute("SELECT * FROM flight_seats WHERE id = " + randomSeat + " AND availability = 1 FOR UPDATE");
            int rows = conn.createStatement().executeUpdate("UPDATE flight_seats SET availability = 0 WHERE id = " + randomSeat + " AND availability = 1");
            tries[this.id-1] = 1;
            
            if (rows == 1) {
                System.out.println("transaction " + id + " booked seat " + randomSeat + ", yeah, commit!");
                conn.createStatement().execute("COMMIT");
                booking[this.id-1] = true;
            } else if (rows == 0) {
                System.out.println("transaction " + id + " could not book seat " + randomSeat + ", try again, rollback!");
                conn.createStatement().execute("ROLLBACK");
                tries[this.id-1]++;
                booking[this.id-1] = false;
                
                boolean success = false;
                
                while (!success) {
                    randomSeat = getRandomSeatNumber(availSeats);
                    conn.createStatement().execute("SELECT * FROM flight_seats WHERE id = " + randomSeat + " AND availability = 1 FOR UPDATE");
                    rows = conn.createStatement().executeUpdate("UPDATE flight_seats SET availability = 0 WHERE id = " + randomSeat + " AND availability = 1");
                    tries[this.id-1]++;
                    
                    if (rows == 1) {
                        System.out.println("transaction " + id + " booked seat " + randomSeat + ", yeah, commit!");
                        conn.createStatement().execute("COMMIT");
                        booking[this.id-1] = true;
                        success = true;
                    } else if (rows == 0) {
                        System.out.println("transaction " + id + " could not book seat " + randomSeat + ", try again, rollback!");
                        conn.createStatement().execute("ROLLBACK");
                        tries[this.id-1]++;
                    }
                }
            }
            System.out.println("transaction " + id + " took " + (System.currentTimeMillis()-startTime) + " ms.");
            
        } catch (SQLException e) {
            System.err.println("transaction " + id + " failed while booking a seat, rollback.");
            try {
                conn.createStatement().execute("ROLLBACK");
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        }
        System.out.println("transaction " + id + " terminated");
        printTriesCounter();
    }

    // compute random seat number
    public int getRandomSeatNumber(ArrayList<Integer> availSeats) {
        
        Random r        = new Random();
        int low         = 0;                           // lower bound for random index
        int high        = availSeats.size();           // upper bound for random index
        int randomIndex = r.nextInt(high-low) + low;   // compute random index
        int randomSeat  = availSeats.get(randomIndex); // select (random) seat number
        System.out.println("SEAT: " + randomSeat);
        
        return randomSeat;
    }

    // print booking states
    public void printBookingState() {        
        for (int i = 0; i < booking.length; i++) {
            System.out.println((i+1) + " booked? " + booking[i]);
        }
    }
    
    // print counted tries
    public void printTriesCounter() {        
        for (int i = 0; i < tries.length; i++) {
            System.out.println((i+1) + " needed " + tries[i] + " tries.");
        }
    }
}

public class ExperimentA {
    
    public static void main(String[] args) {

        String host     = "localhost"; // for local use
        String pwd      = "abc";
        String user     = "testuser";
        Connection conn = null;
        int mode        = 1; // mode: 0 - read committed, 1 - serializable
        String level    = "";
        
        if (mode == 0)
            level = "READ COMMITTED";
        else if (mode == 1)
            level = "SERIALIZABLE";
        
        // "build" url out of user, pwd and host
        String url = "jdbc:oracle:thin:" + user + "/" + pwd + "@" + host;
        
        // setup jdbc driver and connection to database
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            System.err.println("Driver found.");
            conn = DriverManager.getConnection(url, user, pwd);
            System.err.println("Connection established.");
        } catch (ClassNotFoundException e) {
            System.err.println("Oracle JDBC Driver not found ... ");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Could not setup connection.");
            e.printStackTrace();
        }

        try {
            conn.createStatement().execute("UPDATE flight_seats SET availability = 1");
            conn.createStatement().execute("SET TRANSACTION ISOLATION LEVEL " + level);
            System.err.println("transaction isolation level was set to " + level);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        long begin = System.currentTimeMillis();   // for measuring runtime in the end
        int numThreads      = 200;                 // There are 200 customers trying to book a seat. 
        int[] kTravelAgents = {1, 2, 4, 6, 8, 10}; // The number of travel agents is a parameter k, where k in {1, 2, 4, 6, 8, 10}
        int maxConcurrent   = kTravelAgents[0];    // Perform the experiment for each k.

        // create numThreads transactions - Each customer books a seat in a separate transaction.
        BookingTransaction[] trans = new BookingTransaction[numThreads];
        
        for (int i = 0; i < trans.length; i++) {
            int qry  = 1;
            trans[i] = new BookingTransaction(i + 1, conn, qry, mode);
        }
        
        // start all transactions using a thread pool 
        // Imitate multiple travel agents that book the seats for the customers. 
        // k is the number of threads performing the booking transactions. 
        // One thread makes one reservation at a time.
        ExecutorService pool = Executors.newFixedThreadPool(maxConcurrent);
        
        for (int i = 0; i < trans.length; i++) {
            // ececute transaction i
            pool.execute(trans[i]);
        }
        pool.shutdown(); // end program after all transactions are done
        
        // check for termination
        boolean ok = true;        
        do {
            if (pool.isTerminated()) {        
                long end = System.currentTimeMillis();
                System.out.println("Time needed: " + (end - begin));
                ok = false;
            }
        } while (ok);
        
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
