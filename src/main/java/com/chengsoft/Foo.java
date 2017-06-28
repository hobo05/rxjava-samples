package com.chengsoft;

import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;

/**
 * @author tcheng
 */
@Data
@NoArgsConstructor
@ToString(exclude = "latch")
@Slf4j
public class Foo {
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
