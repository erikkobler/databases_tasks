import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;


public class Serialized {

    public static int num_tasks = 30;
    public static int num_queries_per_task = 3;

    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;
    AtomicInteger tasks_finished;

    public Serialized() {
        tasks_finished = new AtomicInteger(0);
    }

    void executeSQL(String sql, Connection con){
        java.sql.Statement stmt;
        try {
            stmt = con.createStatement();
            stmt.execute(sql);
            System.out.println("Executing "+sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void createDB(){
        try {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            executeSQL("DROP TABLE IF EXISTS blocking_data;",con);
            executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);",con);

            PreparedStatement ps;
            con.setAutoCommit(false);
            ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            for(int i=0;i<num_tuple;i++){
                ps.setInt(1, i);
                ps.setDouble(2,rand.nextDouble()*max_value);
                ps.setInt(3, 0);
                ps.addBatch();
                ps.clearParameters();
            }

            int[] results = ps.executeBatch();
            con.commit();
            con.close();
            System.out.println("Inserted " + Arrays.stream(results).sum() + " records.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        // Create DB
        Serialized b = new Serialized();
        b.createDB();

        // Create tasks and their queries
        ArrayList<Serialized.Blocker> my_tasks = new ArrayList<>(num_tasks);
        for(int task=0; task<num_tasks; task++){
            my_tasks.add(b.new Blocker(task));
        }

        // Execute tasks serially
        double start, stop;
        start = System.currentTimeMillis();
        for(Blocker task : my_tasks) {
            task.run();  // Execute each task serially
        }
        stop = System.currentTimeMillis();
        System.out.println("[DONE] Task execution after roughly "+(stop-start)+" ms finished: "+b.tasks_finished+" of "+num_tasks);
        System.out.println("Number of queries per task: "+num_queries_per_task);
    }

    /**
     * This task attempts to update major parts of the database.
     */
    public class Blocker implements Runnable  {
        final int task;
        final double [] start_range;
        final double [] stop_range;

        public Blocker(int id){
            this.task = id;
            this.start_range = new double[num_queries_per_task];
            this.stop_range  = new double[num_queries_per_task];
            for(int q=0; q<num_queries_per_task; q++) {
                stop_range[q]  = rand.nextDouble()*max_value;
                start_range[q] = rand.nextDouble()*stop_range[q];
            }
        }

        @Override
        public void run() {
            Connection con = null;
            try {
                con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
                con.setAutoCommit(false);
                con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

                PreparedStatement ps = con.prepareStatement(
                        "UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_value >= ? AND data_value <= ?;",
                        ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE
                );

                for(int q=0; q<num_queries_per_task; q++) {
                    ps.setDouble(1, rand.nextDouble()*max_value);
                    ps.setInt(2, task);
                    ps.setDouble(3, start_range[q]);
                    ps.setDouble(4, stop_range[q]);
                    ps.execute();
                }
                con.commit();
                int num_finished = tasks_finished.incrementAndGet();
                System.out.println("Done task "+task+". I am finisher number "+num_finished);
            } catch (SQLException e) {
                System.err.println("Task "+task+" encountered an error: "+e.getMessage());
                e.printStackTrace();
            } finally {
                try {
                    if (con != null && !con.isClosed()){
                        con.close();
                        System.out.println("Connection closed for task "+task);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
