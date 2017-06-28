package com.chengsoft;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.UUID;
import java.util.concurrent.*;

import static com.chengsoft.MarkerConstants.NO_THREAD;

/**
 * @author tcheng
 */
@SuppressWarnings("Duplicates")
@Slf4j
public class AsyncTest {

    private static final int SLOW = 5000;
    private static final int NORMAL = 2000;
    private static final int FAST = 500;

    private ExecutorService pool;

    @Before
    public void before() {
        pool = Executors.newFixedThreadPool(10, new ThreadFactoryBuilder()
                .setNameFormat("Fixed Pool-%d")
                .build());

        // Track the time
        Observable.interval(500, TimeUnit.MILLISECONDS)
                .subscribe(i -> log.info(NO_THREAD, String.format("%.1f seconds", (i + 1) * 0.5)));
    }

    /**
     * Subscribe to an async method twice and evaluate twice
     *
     * @throws InterruptedException
     */
    @Test
    public void testEvaluateEveryTimeAsync() throws InterruptedException {
        Observable<String> randomUuid = Observable.fromCallable(() -> {
            String uuid = UUID.randomUUID().toString();
            log.info("Generated [UUID={}]", uuid);
            return uuid;
        }).subscribeOn(Schedulers.newThread());

        CountDownLatch latch = new CountDownLatch(2);

        randomUuid.subscribe(uuid -> {
            latch.countDown();
            log.info(uuid);
        });
        randomUuid.subscribe(uuid -> {
            latch.countDown();
            log.info(uuid);
        });

        latch.await();
    }

    /**
     * Subscribe to an async method twice and evaluate once using {@link Future}
     *
     * @throws InterruptedException
     */
    @Test
    public void testEvaluateOnceAsyncUsingFuture() throws InterruptedException {

        Observable<String> randomUuid = Observable.from(pool.submit(() -> {
            String uuid = UUID.randomUUID().toString();
            log.info("Generated [UUID={}]", uuid);
            return uuid;
        }));

        CountDownLatch latch = new CountDownLatch(2);

        randomUuid.subscribe(uuid -> {
            latch.countDown();
            log.info(uuid);
        });
        randomUuid.subscribe(uuid -> {
            latch.countDown();
            log.info(uuid);
        });

        latch.await();
    }

    /**
     * Subscribe to an async method twice and evaluate once using {@link Future}
     *
     * @throws InterruptedException
     */
    @Test
    public void testEvaluateOnceAsyncUsingAsync() throws InterruptedException {

//        Observable<String> randomUuid = Async.start(() ->
//            String uuid = UUID.randomUUID().toString();
//            log.info("Generated [UUID={}]", uuid);
//            return uuid;
//        });
//
//        CountDownLatch latch = new CountDownLatch(2);
//
//        randomUuid.subscribe(uuid -> {
//            latch.countDown();
//            log.info(uuid);
//        });
//        randomUuid.subscribe(uuid -> {
//            latch.countDown();
//            log.info(uuid);
//        });
//
//        latch.await();
    }

    public @NotNull Object nullable = "abc";

    @Test
    public void test() {
//        @NotNull Object test = nullable;  //error on this line
        nullable = null;
        System.out.println(nullable);
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
