package no.ssb.dapla.blueprintexecution.model;

import io.helidon.common.reactive.Single;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

public class NotebookExecutionTest {


    @Test
    public void testExecution() throws ExecutionException, InterruptedException {
        // Create a graph of job and execute them.
        var jobA = new TestJob("job A");
        var jobB = new TestJob("job B");
        var jobC = new TestJob("job C");

        var jobD = new TestJob("job D", jobA, jobB);
        var jobE = new TestJob("job E", jobB, jobC);

        var jobF = new TestJob("job F", jobD, jobE);
        var jobG = new TestJob("job G", jobE);

        var jobH = new TestJob("job H", jobF);
        var jobI = new TestJob("job I", jobG);

        var jobIResult = jobI.executeJob();
        var jobHResult = jobI.executeJob();

        assertThat(jobIResult.toCompletableFuture().isDone()).isFalse();
        assertThat(jobHResult.toCompletableFuture().isDone()).isFalse();
        jobI.executePrevious();

        jobIResult.get();
        jobHResult.get();

        assertThat(jobIResult.toCompletableFuture().isDone()).isTrue();
        assertThat(jobHResult.toCompletableFuture().isDone()).isTrue();

    }

    public static class TestJob extends AbstractJob {

        private final CountDownLatch latch = new CountDownLatch(1);
        private final String name;

        public TestJob(String name, TestJob... previousNodes) {
            super(previousNodes);
            this.name = name;
        }

        public void execute() {
            latch.countDown();
        }

        public void executePrevious() {
            for (AbstractJob previousNode : previousNodes) {
                ((TestJob) previousNode).executePrevious();
            }
            execute();
        }

        @Override
        protected Single<AbstractJob> startJob() {
            Single<AbstractJob> single = Single.create(CompletableFuture.supplyAsync(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return this;
            }));
            return single;
        }

        @Override
        public String toString() {
            return "ManualJobNode{" + "name='" + name + '\'' + ", " + (latch.getCount() == 0 ? "executed" : "waiting") + '}';
        }
    }
}