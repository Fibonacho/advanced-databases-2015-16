/*
 * Example code for Assignment 6 (concurrency tuning) of the course:
 * 
 * Database Tuning
 * Department of Computer Science
 * University of Salzburg, Austria
 * 
 * Lecturer: Nikolaus Augsten
 */

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** 
 * Dummy transaction that prints a start message, waits for a random time 
 * (up to 100ms) and finally prints a status message at termination.
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
        
        // replace this with a transaction
        int ms = (int)(Math.random()*100);
        try {
            String query = "";
            // QUERY 1
            if (this.qry == 1) {
                System.out.println("\nQUERY " + id);
                
                String e = "SELECT balance FROM accounts WHERE account = " + id + ";";
                //System.out.println("e <- " + e);                
                e = db.executeQuery(e);
                //System.out.println("e = " + e);
                
                query = "UPDATE accounts SET balance = " + e + " + 1 WHERE account = " + id + ";";
                if (mode == 0)
                    db.sendReadCommitted(query);
                else if (mode == 1)
                    db.sendSerializable(query);
                
                String c = "SELECT balance FROM accounts WHERE account = 0;";
                //System.out.println("c <- " + c);
                c = db.executeQuery(c);
                //System.out.println("c = " + c);
                
                query = "UPDATE accounts SET balance = " + c + " - 1 WHERE account = 0;";
                if (mode == 0)
                    db.sendReadCommitted(query);
                else if (mode == 1)
                    db.sendSerializable(query);            
            // QUERY 2
            } else if (this.qry == 2) {
                query = "UPDATE accounts SET balance = balance + 1 WHERE account = " + id + ";\n"
                        + "UPDATE accounts SET balance = balance - 1 WHERE account = 0;";
                
                if (mode == 0)
                    db.sendReadCommitted(query);
                else if (mode == 1)
                    db.sendSerializable(query);
            }            
            sleep(ms);
        } catch (Exception e) {};
        
        System.out.println("transaction " + id + " terminated");
    }
}

/**
 * <p>
 * Run numThreads transactions, where at most maxConcurrent transactions 
 * can run in parallel.
 * 
 * <p>params: numThreads maxConcurrent
 *
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
        long begin = System.currentTimeMillis();
        
        /** There are 200 customers trying to book a seat. 
         *  Each customer books a seat in a separate transaction.
         *  Imitate multiple travel agents that book the seats for the customers. 
         *  The number of travel agents is a parameter k, where k in {1,2,4,6,8,10}. 
         */      
        int numThreads = 200;
        int maxConcurrent = Integer.parseInt(args[1]);
        int[] kTravelAgents = {1, 2, 4, 6, 8, 10};

        // create numThreads transactions
        Transaction[] trans = new Transaction[numThreads];
        
        for (int i = 0; i < trans.length; i++) {
            // mode: 0 - read committed, 1 - serializable
            int mode = 1;
            int qry  = 1;
            trans[i] = new Transaction(i + 1, db, qry, mode);
        }
        
        // start all transactions using a thread pool 
        ExecutorService pool = Executors.newFixedThreadPool(maxConcurrent);                
        for (int i = 0; i < trans.length; i++) {
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
    - There are 200 customers trying to book a seat. Each customer books a seat in a separate transaction.
    - Imitate multiple travel agents that book the seats for the customers. The number of travel agents is a parameter k, where k in {1,2,4,6,8,10}. Perform the experiment for each k. k is the number of threads performing the booking transactions. One thread makes one reservation at a time.
    - Evaluate two versions of the booking transaction : a) as a single transaction including all three steps, and b) split into two smaller transactions: b1) retrieving available seats, b2) securing a seat. In case b), the decision time is not part of transactions but has to be considered.
    - Perform the experiment for two isolation levels: 'read committed' and 'serializable'.
    - Set explicitly row locking. If this is not possible, describe the locking mechanism.
    - Restart the transactions until they commit (all customers book a seat).
    
    Measure the following properties:
    - Total time required for all customers to book a seat (for each k, isolation level, and transaction version). Show the results on a line plot where x axis shows parameter k and y axis shows time.
    - Min, max, and avg number of times a customer had to try to book a seat until he got it (number of times a single transaction had to be restarted). Show the results in a table.
    
    Briefly describe the outcome and explain the differences between the transaction versions and isolation levels.
 */
