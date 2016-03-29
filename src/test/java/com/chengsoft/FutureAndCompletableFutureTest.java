package com.chengsoft;

import com.google.common.collect.ImmutableList;
import lombok.*;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author tcheng
 */
@SuppressWarnings("Duplicates")
public class FutureAndCompletableFutureTest {

    private static final int SLOW = 5000;
    private static final int NORMAL = 2000;
    private static final int FAST = 500;

    /**
     * @author tcheng
     */
    @Data
    @NoArgsConstructor
    @ToString(exclude = "latch")
    public static class Foo {
        @Getter(AccessLevel.NONE)
        @Setter(AccessLevel.NONE)
        private CountDownLatch latch = new CountDownLatch(3);
        private String partOne;
        private String partTwo;
        private String partThree;

        public Foo(String partOne, String partTwo, String partThree) {
            this.partOne = partOne;
            this.partTwo = partTwo;
            this.partThree = partThree;
        }

        public void setPartOne(String partOne) {
            System.out.println("Setting Part 1");
            this.partOne = partOne;
            latch.countDown();
        }

        public void setPartTwo(String partTwo) {
            System.out.println("Setting Part 2");
            this.partTwo = partTwo;
            latch.countDown();
        }

        public void setPartThree(String partThree) {
            System.out.println("Setting Part 3");
            this.partThree = partThree;
            latch.countDown();
        }

        public void waitForParts() {
            try {
                System.out.println("Block until all parts have been set");
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Before
    public void before() {
        // Track the time
        Observable.interval(500, TimeUnit.MILLISECONDS)
                .subscribe(i -> System.out.printf("%.2f seconds%n", (i + 1) * 0.5));
    }

    @Test
    public void testBlockingFuture() throws ExecutionException, InterruptedException {
        Foo foo = new Foo();

        ExecutorService threadPool = Executors.newCachedThreadPool();

        Future<String> partOne = threadPool.submit(() -> {
            System.out.println("Start Part 1");
            uncheckedSleep(SLOW);
            System.out.println("Part 1 Ready");
            return "a";
        });

        Future<String> partTwo = threadPool.submit(() -> {
            System.out.println("Start Part 2");
            uncheckedSleep(NORMAL);
            System.out.println("Part 2 Ready");
            return "b";
        });

        Future<String> partThree = threadPool.submit(() -> {
            System.out.println("Start Part 3");
            uncheckedSleep(SLOW);
            System.out.println("Part 3 Ready");
            return "c";
        });

        // Future.get() blocks until result is returned
        // Part 1 blocks Part 2 and Part 3 from getting set even though they're ready
        System.out.println("Blocking for Part 1");
        foo.setPartOne(partOne.get());

        System.out.println("Blocking for Part 2");
        foo.setPartTwo(partTwo.get());

        System.out.println("Blocking for Part 3");
        foo.setPartThree(partThree.get());

        System.out.println("Assembled " + foo);
        System.out.println("Done Setting Parts");
    }

    @Test
    public void testPollingFuture() throws ExecutionException, InterruptedException {
        Foo foo = new Foo();

        ExecutorService threadPool = Executors.newCachedThreadPool();

        Callable<String> partOne = () -> {
            System.out.println("Start Part 1");
            uncheckedSleep(SLOW);
            System.out.println("Part 1 Ready");
            return "a";
        };

        Callable<String> partTwo = () -> {
            System.out.println("Start Part 2");
            uncheckedSleep(NORMAL);
            System.out.println("Part 2 Ready");
            return "b";
        };

        Callable<String> partThree = () -> {
            System.out.println("Start Part 3");
            uncheckedSleep(SLOW);
            System.out.println("Part 3 Ready");
            return "c";
        };

        // Old school way of polling until done
        final List<Future<String>> futures = new ImmutableList.Builder<Future<String>>()
                .add(threadPool.submit(partOne))
                .add(threadPool.submit(partTwo))
                .add(threadPool.submit(partThree))
                .build();

        boolean doneProcessing = false;
        while (!doneProcessing) {
            Boolean allReturned = true;
            for (Future<String> future : futures) {
                Thread.sleep(500);
                System.out.println("Check if done or not");
                allReturned = Boolean.logicalAnd(allReturned, future.isDone());

                // After the future is done, we use the result somehow
                // but the problem is that we don't know which part it is and we will have to add
                // meta data to the result since the results are all of the same type
                if (future.isDone()) {
                    System.out.println("Do something with: " + future.get());
                }

            }
            System.out.println("All results returned? " + allReturned);
            doneProcessing = allReturned;
        }

        System.out.println("Assembled " + foo);
        System.out.println("Done Setting Parts");
    }

    @Test
    public void testInvokeAllFuture() throws ExecutionException, InterruptedException {
        Foo foo = new Foo();

        ExecutorService threadPool = Executors.newCachedThreadPool();

        Callable<String> partOne = () -> {
            System.out.println("Start Part 1");
            uncheckedSleep(SLOW);
            System.out.println("Part 1 Ready");
            return "a";
        };

        Callable<String> partTwo = () -> {
            System.out.println("Start Part 2");
            uncheckedSleep(NORMAL);
            System.out.println("Part 2 Ready");
            return "b";
        };

        Callable<String> partThree = () -> {
            System.out.println("Start Part 3");
            uncheckedSleep(SLOW);
            System.out.println("Part 3 Ready");
            return "c";
        };

        // Block until all futures are done
        final List<Callable<String>> futures = ImmutableList.of(partOne, partTwo, partThree);
        System.out.println("Use ExecutorService.invokeAll() to block until all Futures are done");
        List<Future<String>> futureResults = threadPool.invokeAll(futures);

        // Now loop through and somehow set the parts
        for (Future<String> future : futureResults) {
            System.out.println("Do something with: " + future.get());
        }

        System.out.println("Assembled " + foo);
        System.out.println("Done Setting Parts");
    }

    @Test
    public void testBlockingCompletableFuture() throws ExecutionException, InterruptedException {
        Foo foo = new Foo();

        CompletableFuture<String> partOne = CompletableFuture.supplyAsync(() -> {
            System.out.println("Start Part 1");
            uncheckedSleep(SLOW);
            System.out.println("Part 1 Ready");
            return "a";
        });

        CompletableFuture<String> partTwo = CompletableFuture.supplyAsync(() -> {
            System.out.println("Start Part 2");
            uncheckedSleep(NORMAL);
            System.out.println("Part 2 Ready");
            return "b";
        });

        CompletableFuture<String> partThree = CompletableFuture.supplyAsync(() -> {
            System.out.println("Start Part 3");
            uncheckedSleep(FAST);
            System.out.println("Part 3 Ready");
            return "c";
        });

        // CompletableFuture.get() blocks until result is returned
        // Part 1 blocks Part 2 and Part 3 from getting set even though they're ready
        System.out.println("Blocking for Part 1");
        foo.setPartOne(partOne.get());

        System.out.println("Blocking for Part 2");
        foo.setPartTwo(partTwo.get());

        System.out.println("Blocking for Part 3");
        foo.setPartThree(partThree.get());

        System.out.println("Assembled " + foo);
        System.out.println("Done Setting Parts");
    }


    @Test
    public void testNonBlockingCompletableFuture() throws ExecutionException, InterruptedException {
        Foo foo = new Foo();

        CompletableFuture.supplyAsync(() -> {
            System.out.println("Start Part 1");
            uncheckedSleep(SLOW);
            System.out.println("Part 1 Ready");
            return "a";
        })
                // Callback method that sets whenever the part is ready
                .thenAccept(result -> foo.setPartOne(result));

        CompletableFuture.supplyAsync(() -> {
            System.out.println("Start Part 2");
            uncheckedSleep(NORMAL);
            System.out.println("Part 2 Ready");
            return "b";
        }).thenAccept(result -> foo.setPartTwo(result));

        CompletableFuture.supplyAsync(() -> {
            System.out.println("Start Part 3");
            uncheckedSleep(FAST);
            System.out.println("Part 3 Ready");
            return "c";
        }).thenAccept(result -> foo.setPartThree(result));

        // artificially block
        foo.waitForParts();

        System.out.println("Assembled " + foo);
        System.out.println("Done Setting Parts");
    }

    @Test
    public void testBlockingObservable() throws ExecutionException, InterruptedException {

        Observable<String> partOne = Observable.fromCallable(() -> {
            System.out.println("Start Part 1");
            uncheckedSleep(SLOW);
            System.out.println("Part 1 Ready");
            return "a";
        });

        Observable<String> partTwo = Observable.fromCallable(() -> {
            System.out.println("Start Part 2");
            uncheckedSleep(NORMAL);
            System.out.println("Part 2 Ready");
            return "b";
        });

        Observable<String> partThree = Observable.fromCallable(() -> {
            System.out.println("Start Part 3");
            uncheckedSleep(FAST);
            System.out.println("Part 3 Ready");
            return "c";
        });

        // No need to block as Observable.subscribe() by default blocks
//        foo.waitForParts();

        // Zip together results into a foo observable
        System.out.println("Create Zip");
        Observable<Foo> fooObservable = Observable.zip(partOne, partTwo, partThree,
                (partOneResult, partTwoResult, partThreeResult) -> {
                    System.out.println("Put together a new Foo with " + Arrays.asList(partOneResult, partTwoResult, partThreeResult));
                    return new Foo(partOneResult, partTwoResult, partThreeResult);
                });
        System.out.println("After Create Zip");

        // Print out once foo has been created
        fooObservable.subscribe(foo -> {
            System.out.println("Assembled " + foo);
        });

        System.out.println("Done Setting Parts");
    }

    @Test
    public void testNonBlockingObservable() throws ExecutionException, InterruptedException {
        AtomicInteger firstCount = new AtomicInteger(0);
        AtomicInteger secondCount = new AtomicInteger(0);
        AtomicInteger threeCount = new AtomicInteger(0);

        Observable<String> partOne = Observable.fromCallable(() -> {
            System.out.println("Part 1 iteration: " + firstCount.getAndIncrement());
            System.out.println("Start Part 1");
            uncheckedSleep(SLOW);
            System.out.println("Part 1 Ready");
            return "a";
        }).subscribeOn(Schedulers.newThread());

//        Observable<String> partTwo = Observable.fromCallable(() -> {
//            System.out.println("Part 2 iteration: " + secondCount.getAndIncrement());
//            System.out.println("Start Part 2");
//            uncheckedSleep(NORMAL);
//            System.out.println("Part 2 Ready");
//            return "b";
//        }).subscribeOn(Schedulers.newThread());

        Observable<String> partTwo = Observable.just("devi", "tim", "erik")
                .subscribeOn(Schedulers.newThread());

        Observable<String> partThree = Observable.fromCallable(() -> {
            System.out.println("Part 3 iteration: " + threeCount.getAndIncrement());
            System.out.println("Start Part 3");
            uncheckedSleep(FAST);
            System.out.println("Part 3 Ready");
            if (threeCount.get() == 1) {
                System.err.println("Fail on the first try!");
                throw new RuntimeException("test");
            }
            return "c";
        }).subscribeOn(Schedulers.newThread());

        // Zip together results into a foo observable
        System.out.println("Create Zip");
        Observable<Foo> fooObservable = Observable.zip(partOne, partTwo, partThree,
                (partOneResult, partTwoResult, partThreeResult) -> {
                    System.out.println("Put together a new Foo with " + Arrays.asList(partOneResult, partTwoResult, partThreeResult));
                    return new Foo(partOneResult, partTwoResult, partThreeResult);
                });
        System.out.println("After Create Zip");

        CountDownLatch latch = new CountDownLatch(1);

        // Print out once foo has been created
        fooObservable
//                .doOnTerminate(() -> latch.countDown())
                .retry()
//                .onErrorResumeNext()
                .subscribe(foo -> {
                    System.out.println("Assembled " + foo);
                }, Throwable::printStackTrace);

//        System.out.println(fooObservable.toBlocking().single());

        // Artificial block
//        latch.await();
        Thread.sleep(10000);

        System.out.println("Done Setting Parts");
    }

    private void uncheckedSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
