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
import java.util.HashSet;

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
        System.out.println("Deadlocks observed: " + b.deadlock_counter);
        System.out.println("Max number of retries for a task: " + b.max_retry_counter);
        System.out.println("Number of queries per task: " + num_queries_per_task);
        System.out.println("Number of Threads: " + b.nThreads);
    }

    public class Blocker implements Runnable {
        final int task;
        final double p;
        final int[] data_ids; // For random data_ids

        public Blocker(int id) {
            this.task = id;
            this.p = rand.nextDouble(); // Generate p in [0,1]

            if (p < 0.7 || p >= 0.8) {
                data_ids = new int[num_queries_per_task];
                HashSet<Integer> used_ids = new HashSet<>();
                for (int q = 0; q < num_queries_per_task; q++) {
                    int data_id;
                    do {
                        data_id = rand.nextInt(num_tuple);
                    } while (used_ids.contains(data_id));
                    used_ids.add(data_id);
                    data_ids[q] = data_id;
                }
            } else {
                data_ids = null; // Not needed for full table scan
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
            } catch (SQLException e) {
                e.printStackTrace();
            }

            do {
                try {
                    // Acquire lock for write transactions after a deadlock
                    if (p >= 0.8 && retries > 0) {
                        lock.lock();
                    }

                    if (p < 0.7) {
                        // Read-only multi-point query
                        double sum = 0;
                        PreparedStatement ps = con.prepareStatement("SELECT data_value FROM blocking_data WHERE data_id = ?");
                        for (int q = 0; q < num_queries_per_task; q++) {
                            ps.setInt(1, data_ids[q]);
                            ResultSet rs = ps.executeQuery();
                            if (rs.next()) {
                                sum += rs.getDouble(1);
                            }
                            rs.close();
                        }
                        con.commit();
                        int num_finished = tasks_finished.incrementAndGet();
                        System.out.println("Task " + task + " (Read multi-point query). Sum: " + sum + ". I am finisher number " + num_finished);
                        success = true;
                    } else if (p >= 0.7 && p < 0.8) {
                        // Full table read-only scan
                        PreparedStatement ps = con.prepareStatement("SELECT SUM(data_value) FROM blocking_data");
                        ResultSet rs = ps.executeQuery();
                        double sum = 0;
                        if (rs.next()) {
                            sum = rs.getDouble(1);
                        }
                        rs.close();
                        con.commit();
                        int num_finished = tasks_finished.incrementAndGet();
                        System.out.println("Task " + task + " (Full table scan). Sum: " + sum + ". I am finisher number " + num_finished);
                        success = true;
                    } else {
                        // p >= 0.8, write query
                        PreparedStatement ps = con.prepareStatement("UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?");
                        for (int q = 0; q < num_queries_per_task; q++) {
                            ps.setDouble(1, rand.nextDouble() * max_value);
                            ps.setInt(2, task);
                            ps.setInt(3, data_ids[q]);
                            ps.executeUpdate();
                        }
                        con.commit();
                        int num_finished = tasks_finished.incrementAndGet();
                        System.out.println("Done task " + task + " (Write). I am finisher number " + num_finished);
                        success = true;
                    }

                    // Release lock if held
                    if (p >= 0.8 && retries > 0 && lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }

                    con.close();
                } catch (SQLException e) {
                    System.err.println("Task " + task + " encountered an error: " + e.getMessage());
                    retries++;
                    deadlock_counter.incrementAndGet();
                    max_retry_counter.set(Math.max(max_retry_counter.get(), retries));
                    System.out.println("Task " + task + " - Rollback and try again. #Retries: " + retries);
                    executeSQL("ROLLBACK", con);
                    try {
                        // Randomized back-off to reduce contention
                        int randomized = rand.nextInt(nThreads * 1000);
                        Thread.sleep(100 + randomized);
                        System.out.println("Task: " + task + " Randomized back-off time: " + randomized + "ms");
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                    // Lock before retrying write transactions
                    if (p >= 0.8 && retries > 0) {
                        lock.lock();
                    }
                } finally {
                    // Ensure lock is released if an exception occurs
                    if (p >= 0.8 && lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }
                }
            } while (!success);
        }
    }
}
