////import org.junit.jupiter.api.Test;
//import java.util.ArrayList;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ThreadPoolExecutor;
////import static org.junit.jupiter.api.Assertions.assertEquals;
//
//public class NoLiveLocksTest {
//
//    @org.junit.Test
//    public void testNoLiveLocks() {
//        NoLiveLocks noLiveLocks = new NoLiveLocks();
//        noLiveLocks.createDB();
//
//        ArrayList<NoLiveLocks.Blocker> tasks = new ArrayList<>(NoLiveLocks.num_tasks);
//        for (int task = 0; task < NoLiveLocks.num_tasks; task++) {
//            tasks.add(noLiveLocks.new Blocker(task));
//        }
//
//        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
//        for (NoLiveLocks.Blocker task : tasks) {
//            executor.execute(task);
//        }
//        executor.shutdown();
//
//        while (!executor.isTerminated()) {
//            try {
//                Thread.sleep(100);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//            }
//        }
//
//        assertEquals(NoLiveLocks.num_tasks, noLiveLocks.tasks_finished.get(), "Not all tasks finished successfully");
//    }
//}