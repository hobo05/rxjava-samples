package com.chengsoft;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.util.async.Async;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.chengsoft.MarkerConstants.*;

/**
 * @author tcheng
 */
@SuppressWarnings("Duplicates")
@Slf4j
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
            log.info("Setting Part 1");
            this.partOne = partOne;
            latch.countDown();
        }

        public void setPartTwo(String partTwo) {
            log.info("Setting Part 2");
            this.partTwo = partTwo;
            latch.countDown();
        }

        public void setPartThree(String partThree) {
            log.info("Setting Part 3");
            this.partThree = partThree;
            latch.countDown();
        }

        public void waitForParts() {
            try {
                log.info("Block until all parts have been set");
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
                .subscribe(i -> log.info(NO_THREAD, String.format("%.1f seconds", (i + 1) * 0.5)));
    }

    @Test
    public void testBlockingFuture() throws ExecutionException, InterruptedException {
        Foo foo = new Foo();

        ExecutorService threadPool = Executors.newCachedThreadPool();

        Future<String> partOne = threadPool.submit(() -> {
            log.info("Start Part 1");
            uncheckedSleep(SLOW);
            log.info("Part 1 Ready");
            return "a";
        });

        Future<String> partTwo = threadPool.submit(() -> {
            log.info("Start Part 2");
            uncheckedSleep(NORMAL);
            log.info("Part 2 Ready");
            return "b";
        });

        Future<String> partThree = threadPool.submit(() -> {
            log.info("Start Part 3");
            uncheckedSleep(SLOW);
            log.info("Part 3 Ready");
            return "c";
        });

        // Future.get() blocks until result is returned
        // Part 1 blocks Part 2 and Part 3 from getting set even though they're ready
        log.info("Blocking for Part 1");
        foo.setPartOne(partOne.get());

        log.info("Blocking for Part 2");
        foo.setPartTwo(partTwo.get());

        log.info("Blocking for Part 3");
        foo.setPartThree(partThree.get());

        log.info("Assembled " + foo);
        log.info("Done Setting Parts");
    }

    @Test
    public void testPollingFuture() throws ExecutionException, InterruptedException {
        Foo foo = new Foo();

        ExecutorService threadPool = Executors.newCachedThreadPool();

        Callable<String> partOne = () -> {
            log.info("Start Part 1");
            uncheckedSleep(SLOW);
            log.info("Part 1 Ready");
            return "a";
        };

        Callable<String> partTwo = () -> {
            log.info("Start Part 2");
            uncheckedSleep(NORMAL);
            log.info("Part 2 Ready");
            return "b";
        };

        Callable<String> partThree = () -> {
            log.info("Start Part 3");
            uncheckedSleep(SLOW);
            log.info("Part 3 Ready");
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
                log.info("Check if done or not");
                allReturned = Boolean.logicalAnd(allReturned, future.isDone());

                // After the future is done, we use the result somehow
                // but the problem is that we don't know which part it is and we will have to add
                // meta data to the result since the results are all of the same type
                if (future.isDone()) {
                    log.info("Do something with: " + future.get());
                }

            }
            log.info("All results returned? " + allReturned);
            doneProcessing = allReturned;
        }

        log.info("Assembled " + foo);
        log.info("Done Setting Parts");
    }

    @Test
    public void testInvokeAllFuture() throws ExecutionException, InterruptedException {
        Foo foo = new Foo();

        ExecutorService threadPool = Executors.newCachedThreadPool();

        Callable<String> partOne = () -> {
            log.info("Start Part 1");
            uncheckedSleep(SLOW);
            log.info("Part 1 Ready");
            return "a";
        };

        Callable<String> partTwo = () -> {
            log.info("Start Part 2");
            uncheckedSleep(NORMAL);
            log.info("Part 2 Ready");
            return "b";
        };

        Callable<String> partThree = () -> {
            log.info("Start Part 3");
            uncheckedSleep(SLOW);
            log.info("Part 3 Ready");
            return "c";
        };

        // Block until all futures are done
        final List<Callable<String>> futures = ImmutableList.of(partOne, partTwo, partThree);
        log.info("Use ExecutorService.invokeAll() to block until all Futures are done");
        List<Future<String>> futureResults = threadPool.invokeAll(futures);

        // Now loop through and somehow set the parts
        for (Future<String> future : futureResults) {
            log.info("Do something with: " + future.get());
        }

        log.info("Assembled " + foo);
        log.info("Done Setting Parts");
    }

    @Test
    public void testBlockingCompletableFuture() throws ExecutionException, InterruptedException {
        Foo foo = new Foo();

        CompletableFuture<String> partOne = CompletableFuture.supplyAsync(() -> {
            log.info("Start Part 1");
            uncheckedSleep(SLOW);
            log.info("Part 1 Ready");
            return "a";
        });

        CompletableFuture<String> partTwo = CompletableFuture.supplyAsync(() -> {
            log.info("Start Part 2");
            uncheckedSleep(NORMAL);
            log.info("Part 2 Ready");
            return "b";
        });

        CompletableFuture<String> partThree = CompletableFuture.supplyAsync(() -> {
            log.info("Start Part 3");
            uncheckedSleep(FAST);
            log.info("Part 3 Ready");
            return "c";
        });

        // CompletableFuture.get() blocks until result is returned
        // Part 1 blocks Part 2 and Part 3 from getting set even though they're ready
        log.info("Blocking for Part 1");
        foo.setPartOne(partOne.get());

        log.info("Blocking for Part 2");
        foo.setPartTwo(partTwo.get());

        log.info("Blocking for Part 3");
        foo.setPartThree(partThree.get());

        log.info("Assembled " + foo);
        log.info("Done Setting Parts");
    }


    @Test
    public void testNonBlockingCompletableFuture() throws ExecutionException, InterruptedException {
        Foo foo = new Foo();

        CompletableFuture.supplyAsync(() -> {
            log.info("Start Part 1");
            uncheckedSleep(SLOW);
            log.info("Part 1 Ready");
            return "a";
        })
                // Callback method that sets whenever the part is ready
                .thenAccept(result -> foo.setPartOne(result));

        CompletableFuture.supplyAsync(() -> {
            log.info("Start Part 2");
            uncheckedSleep(NORMAL);
            log.info("Part 2 Ready");
            return "b";
        }).thenAccept(result -> foo.setPartTwo(result));

        CompletableFuture.supplyAsync(() -> {
            log.info("Start Part 3");
            uncheckedSleep(FAST);
            log.info("Part 3 Ready");
            return "c";
        }).thenAccept(result -> foo.setPartThree(result));

        // artificially block
        foo.waitForParts();

        log.info("Assembled " + foo);
        log.info("Done Setting Parts");
    }

    @Test
    public void testBlockingObservable() throws ExecutionException, InterruptedException {

        Observable<String> partOne = Observable.fromCallable(() -> {
            logStartPart(1);
            uncheckedSleep(SLOW);
            log.info("Part 1 Ready");
            return "a";
        });

        Observable<String> partTwo = Observable.fromCallable(() -> {
            logStartPart(2);
            uncheckedSleep(NORMAL);
            log.info("Part 2 Ready");
            return "b";
        });

        Observable<String> partThree = Observable.fromCallable(() -> {
            logStartPart(3);
            uncheckedSleep(FAST);
            log.info("Part 3 Ready");
            return "c";
        });

        // No need to block as Observable.subscribe() by default blocks
//        foo.waitForParts();

        // Zip together results into a foo observable
        log.info("Create Zip");
        Observable<Foo> fooObservable = Observable.zip(partOne, partTwo, partThree,
                (partOneResult, partTwoResult, partThreeResult) -> {
                    log.info("Put together a new Foo with {}", Arrays.asList(partOneResult, partTwoResult, partThreeResult));
                    return new Foo(partOneResult, partTwoResult, partThreeResult);
                });
        log.info("After Create Zip");

        // Print out once foo has been created
        fooObservable.subscribe(foo -> {
            log.info("Assembled " + foo);
        });

        log.info("Done Setting Parts");
    }

    @Test
    public void testNonBlockingWithFuturesObservable() throws ExecutionException, InterruptedException {

        ExecutorService pool = Executors.newFixedThreadPool(3, new ThreadFactoryBuilder()
                .setNameFormat("Fixed Pool-%d")
                .build());
        log.info("Created Fixed Thread Pool");

        Observable<String> partOne = Observable.from(pool.submit(() -> {
            logStartPart(1);
            uncheckedSleep(SLOW);
            log.info("Part 1 Ready");
            return "a";
        }));

        Observable<String> partTwo = Observable.from(pool.submit(() -> {
            logStartPart(2);
            uncheckedSleep(NORMAL);
            log.info("Part 2 Ready");
            return "b";
        }));

        Observable<String> partThree = Observable.from(pool.submit(() -> {
            logStartPart(3);
            uncheckedSleep(FAST);
            log.info("Part 3 Ready");
            return "c";
        }));

        // subscribe() still runs on the main thread and is blocking
        Observable<Foo> fooObservable = Observable.zip(partOne, partTwo, partThree,
                (partOneResult, partTwoResult, partThreeResult) -> {
                    log.info("Put together a new Foo with {}", Arrays.asList(partOneResult, partTwoResult, partThreeResult));
                    return new Foo(partOneResult, partTwoResult, partThreeResult);
                });
        log.info("After Create Zip");

        // Print out once foo has been created
        fooObservable.subscribe(foo -> {
            log.info("Assembled " + foo);
        });

        log.info("Done Setting Parts");
    }

    @Test
    public void testNonBlockingObservableWithAsync() throws ExecutionException, InterruptedException {

        Observable<String> partOne = Async.start(() -> {
            logStartPart(1);
            uncheckedSleep(SLOW);
            log.info("Part 1 Ready");
            return "a";
        }).subscribeOn(Schedulers.newThread());

        Observable<String> partTwo = Async.start(() -> {
            logStartPart(2);
            uncheckedSleep(NORMAL);
            log.info("Part 2 Ready");
            return "b";
        }).subscribeOn(Schedulers.newThread());

        Observable<String> partThree = Async.start(() -> {
            logStartPart(3);
            uncheckedSleep(FAST);
            log.info("Part 3 Ready");
            return "c";
        }).subscribeOn(Schedulers.newThread());

        // subscribe() does NOT run on the main thread so we have to artificially block
        Observable<Foo> fooObservable = Observable.zip(partOne, partTwo, partThree,
                (partOneResult, partTwoResult, partThreeResult) -> {
                    log.info("Put together a new Foo with " + Arrays.asList(partOneResult, partTwoResult, partThreeResult));
                    return new Foo(partOneResult, partTwoResult, partThreeResult);
                });
        log.info("After Create Zip");

        CountDownLatch latch = new CountDownLatch(1);

        // Print out once foo has been created
        fooObservable
                .doOnTerminate(() -> latch.countDown())
                .retry()
                .subscribe(foo -> {
                    log.info("Assembled " + foo);
                }, Throwable::printStackTrace);

        // Artificial block
        latch.await();

        log.info("Done Setting Parts");
    }

    @Test
    public void testNonBlockingObservableWithErrors() throws ExecutionException, InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(10, new ThreadFactoryBuilder()
                .setNameFormat("Fixed Pool-%d")
                .build());
        log.info("Created Fixed Thread Pool");

        AtomicInteger firstCount = new AtomicInteger(0);
        AtomicInteger secondCount = new AtomicInteger(0);
        AtomicInteger threeCount = new AtomicInteger(0);

        Observable<String> partOne = Observable.from(pool.submit(() -> {
            log.info("Part 1 iteration: " + firstCount.getAndIncrement());
            logStartPart(1);
            uncheckedSleep(SLOW);
            log.info("Part 1 Ready");
            return "a";
        }));

        Observable<String> partTwo = Observable.create(subscriber -> {
            subscriber.onNext("devi");
            subscriber.onNext("mike");
            subscriber.onNext("tim");
            subscriber.onCompleted();
        });

        AtomicInteger failureCount = new AtomicInteger(1);
        // Future and Async.start() only evaluates once so it will fail infinite times
        // Observable.fromCallable() will evaluate every single time so it will only fail once

//        Observable<String> partThree = Observable.from(pool.submit(() -> {
        Observable<String> partThree = Async.start(() -> {
            log.info("Part 3 iteration: " + threeCount.getAndIncrement());
            logStartPart(3);
            uncheckedSleep(FAST);
            log.info("Part 3 Ready");
            if (threeCount.get() == 1) {
                log.error("Fail on the first try!");
                throw new RuntimeException("test");
            }
            return "c";
        })
                .retry(5)
                .doOnError(t -> log.error("Failed on Part 3 count {}", failureCount.getAndIncrement()));

        // Zip together results into a foo observable
        log.info("Create Zip");
        Observable<Foo> fooObservable = Observable.zip(partOne, partTwo, partThree,
                (partOneResult, partTwoResult, partThreeResult) -> {
                    log.info("Put together a new Foo with " + Arrays.asList(partOneResult, partTwoResult, partThreeResult));
                    return new Foo(partOneResult, partTwoResult, partThreeResult);
                });
        log.info("After Create Zip");

        // Print out once foo has been created
        fooObservable
                .retry(5)
//                .onErrorResumeNext()
                .subscribe(foo -> {
                    log.info("Assembled " + foo);
                }, t -> log.error("Exception on fooObservable: ", t));

        log.info("Done Setting Parts");
    }

    private void logStartPart(int part) {
        log.info("Start Part {} on", part);
    }

    private void uncheckedSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
