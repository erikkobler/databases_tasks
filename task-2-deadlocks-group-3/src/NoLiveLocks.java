import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author Martin Schaeler martin.schaeler@plus.ac.at
 *
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
 **/

public class NoLiveLocks {
    public static int num_tasks = 30;
    public static int num_queries_per_task = 3;
    int nThreads = 4;

    Connection con;
    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;
    AtomicInteger tasks_finished;
    AtomicInteger max_retry_counter;
    AtomicInteger deadlock_counter;
    ReentrantLock lock = new ReentrantLock(true); // Fair lock to prevent starvation

    public NoLiveLocks() {

        tasks_finished = new AtomicInteger(0);
        max_retry_counter = new AtomicInteger(0);
        deadlock_counter = new AtomicInteger(0);
    }

    void executeSQL(String sql, Connection con) {
        java.sql.Statement stmt;
        try {
            stmt = con.createStatement();
            stmt.execute(sql);
            System.out.println("Executing " + sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void createDB() {
        try {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            executeSQL("DROP TABLE IF EXISTS blocking_data;", con);
            executeSQL("CREATE TABLE blocking_data (data_id integer primary key, data_value float, modified_by integer);", con);

            PreparedStatement ps;
            con.setAutoCommit(false);
            ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            for (int i = 0; i < num_tuple; i++) {
                ps.setInt(1, i);
                ps.setDouble(2, rand.nextDouble() * max_value);
                ps.setInt(3, 0);
                ps.addBatch();
                ps.clearParameters();
            }

            int[] results = ps.executeBatch();
            con.commit();
            con.close();
            System.out.println(Arrays.stream(results).sum());
            //System.out.println(Arrays.toString(results));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // Create DB
        NoLiveLocks b = new NoLiveLocks();
        b.createDB();

        // Create tasks and their queries
        ArrayList<NoLiveLocks.Blocker> my_tasks = new ArrayList<>(num_tasks);
        for (int task = 0; task < num_tasks; task++) {
            my_tasks.add(b.new Blocker(task));
        }

        // Execute tasks
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(b.nThreads); // Do not change the number of threads used
        double start, stop;
        start = System.currentTimeMillis();
        for (Blocker task : my_tasks) {
            executor.execute(task);
        }
        executor.shutdown();

        // Wait for the tasks to finish
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        stop = System.currentTimeMillis();
        System.out.println("[DONE] Task execution after roughly " + (stop - start) + " ms finished: " + b.tasks_finished + " of " + num_tasks);
        System.out.println("Deadlocks observed: "+b.deadlock_counter);
        System.out.println("Max number of retries for a task: "+b.max_retry_counter);
        System.out.println("Number of queries per task: "+num_queries_per_task);
        System.out.println("Number of Threads: "+b.nThreads);
    }

    public class Blocker implements Runnable {
        final int task;
        final double[] start_range;
        final double[] stop_range;

        public Blocker(int id) {
            this.task = id;
            this.start_range = new double[num_queries_per_task];
            this.stop_range = new double[num_queries_per_task];
            for (int q = 0; q < num_queries_per_task; q++) {
                stop_range[q] = rand.nextDouble() * max_value;
                start_range[q] = rand.nextDouble() * stop_range[q];
            }
        }

        @Override
        public void run() {
            int retries = 0;
            boolean success = false;
            Connection con = null;
            con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            try {
                con.setAutoCommit(false);
                con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            } catch(SQLException e) { }

            do {
                try {
                    int q = 0;
                    PreparedStatement ps = con.prepareStatement("UPDATE blocking_data SET data_value = ?, modified_by = " + task + " WHERE data_value >= ? AND data_value <= ?;", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

                    //lock.lock();
                    while (q < num_queries_per_task) {
                        ps.setDouble(1, rand.nextDouble() * max_value);
                        ps.setDouble(2, start_range[q]);
                        ps.setDouble(3, stop_range[q]);

                        ps.execute();
                        q++;
                    }
                    con.commit();
                    //lock.unlock();
                    for(int i=0;i<retries;i++) { //this is part of the alternative solution in NoLiveLocks.java
                        lock.unlock();
                    }
                    int num_finished = tasks_finished.incrementAndGet();
                    con.close();
                    System.out.println("Done task " + task + ". I am finisher number " + num_finished);
                    success = true;

                } catch (SQLException e) {
                    System.err.println("Task " + task + " " + e.getMessage());
                    retries++;
                    deadlock_counter.incrementAndGet();
                    max_retry_counter.set(Math.max(max_retry_counter.get(),retries));
                    if (retries >= 1) { //use this to set a max. number of retries (e.g. retries<5)
                        System.out.println("Task "+task+" - Rollback and try again. #Retries: "+retries);
                        //System.out.println("Task "+task+": ROLLBACK");
                        executeSQL("ROLLBACK",con);
                        try { //this is part of the alternative solution in NoLiveLocks.java
                            int randomized = rand.nextInt(nThreads*1000);
                            Thread.sleep(100 + randomized);
                            //System.out.println("Task: " + task + " The Randomized value was: " + randomized);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                        lock.lock();
                    } else {
                        System.out.println("Task" + task + " failed. Abort.");
                        executeSQL("ROLLBACK",con);
                        try {
                            if (con != null){
                                con.close();
                                System.out.println("Connection closed.");
                            }
                        } catch (SQLException e2) {
                            e2.printStackTrace();
                        }

                        break;
                    }
                }
            } while (!success);
        }
    }
}
