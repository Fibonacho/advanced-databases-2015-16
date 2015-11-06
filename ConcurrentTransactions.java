/*
 * Example code for Assignment 6 (concurrency tuning) of the course:
 * 
 * Database Tuning
 * Department of Computer Science
 * University of Salzburg, Austria
 * 
 * Lecturer: Nikolaus Augsten
 */

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** 
 * Transaction
 */
class Transaction extends Thread {

    // identifier of the transaction
    int id, mode, qry;
    DBConnect db;

    Transaction(int id, DBConnect db, int qry, int mode) {
        this.id   = id;
        this.db   = db;
        this.mode = mode;
        this.qry  = qry;
    }
    
    @Override
    public void run() {
        
        System.out.println("transaction " + id + " started");
        int ms = 1000;

        try {
            String query = "";
            String tableName = "flight_seats";

            if (this.qry == 1) { // QUERY 1
                System.out.println("\nQUERY " + id);
                
                // 1. Retrieve list of available seats
                query = "select id from " + tableName + " where availability = 1";
                ArrayList<String> availSeats = new ArrayList<String>();
                
                if (mode == 0)
                    availSeats = db.retrieveSeatsReadCommitted(query);
                else if (mode == 1)
                    availSeats = db.retrieveSeatsSerializable(query);
                
                System.out.println("Number of available seats: " + availSeats.size());

                // 2. Give the customer some time (decision time is 1 second) to decide on a seat
                sleep(ms);
                Random r        = new Random();
                int low         = 0;                 // lower bound for random index
                int high        = availSeats.size(); // upper bound for random index
                int randomIndex = r.nextInt(high-low) + low; // compute random index

                String randomSeat = availSeats.get(randomIndex); // select (random) seat number
                System.out.println("SEAT: " + randomSeat);

                // 3. Secure a seat (update the availability of the chosen seat to false).
                query = "update " + tableName + " set availability = 0 where id = " + randomSeat;
                
                if (mode == 0)
                    db.sendReadCommitted(query);
                else if (mode == 1)
                    db.sendSerializable(query);

            } else if (this.qry == 2) { // QUERY 2 TODO
                System.out.println("\nQUERY " + id);
            }        
        } catch (Exception e) {};
        
        System.out.println("transaction " + id + " terminated");
    }
}

/**
 * Run numThreads transactions, where at most maxConcurrent transactions can run in parallel.
 * params: numThreads maxConcurrent
 */
public class ConcurrentTransactions {

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
        
        DBConnect db = new DBConnect();
        String tableName = "flight_seats";
        db.dropTable(tableName);
        db.createTableScheme();
        db.fillTable();
        
        long begin = System.currentTimeMillis();
        
        /** There are 200 customers trying to book a seat. 
         *  Each customer books a seat in a separate transaction.
         *  Imitate multiple travel agents that book the seats for the customers. 
         *  Perform the experiment for each k. k is the number of threads performing 
         *  the booking transactions. One thread makes one reservation at a time.
         */
        int numThreads      = 200; 
        // The number of travel agents is a parameter k, where k in {1, 2, 4, 6, 8, 10}
        int[] kTravelAgents = {1, 2, 4, 6, 8, 10};
        int maxConcurrent   = kTravelAgents[3];

        // create numThreads transactions
        Transaction[] trans = new Transaction[numThreads];
        
        for (int i = 0; i < trans.length; i++) {
            // mode: 0 - read committed, 1 - serializable
            int mode = 0;
            int qry  = 1;
            trans[i] = new Transaction(i + 1, db, qry, mode);
        }
        
        // start all transactions using a thread pool 
        ExecutorService pool = Executors.newFixedThreadPool(maxConcurrent);
        
        for (int i = 0; i < trans.length; i++) {
            // ececute transaction i
            pool.execute(trans[i]);
        }
        pool.shutdown(); // end program after all transactions are done
        
        boolean ok = true;        
        do {
            if (pool.isTerminated()) {        
                long end = System.currentTimeMillis();
                System.out.println("Time needed: " + (end - begin));
                ok = false;
            }
        } while (ok);
    }
}

/** Perform the following experiment:
    - There are 200 customers trying to book a seat. 
      Each customer books a seat in a separate transaction.
    - Imitate multiple travel agents that book the seats for the customers. 
      The number of travel agents is a parameter k, where k in 
      {1,2,4,6,8,10}. 
      Perform the experiment for each k. k is the number of threads 
      performing the booking transactions. One thread makes one 
      reservation at a time.
    - Evaluate two versions of the booking transaction : 
      a) as a single transaction including all three steps, and 
      b) split into two smaller transactions: 
         b1) retrieving available seats, 
         b2) securing a seat. In case b), 
         the decision time is not part of transactions but has to be 
         considered.
    - Perform the experiment for two isolation levels: 
      'read committed' and 'serializable'.
    - Set explicitly row locking. 
      If this is not possible, describe the locking mechanism.
    - Restart the transactions until they commit (all customers 
      book a seat).
 */
