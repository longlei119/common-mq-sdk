package com.lachesis.windrangerms.mq;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StopWatch;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * 测试工具类，提供测试过程中常用的工具方法
 */
@Slf4j
public class TestUtils {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * 生成测试消息内容
     * @param prefix 消息前缀
     * @param index 消息索引
     * @return 测试消息
     */
    public static String generateTestMessage(String prefix, int index) {
        return String.format("%s_%d_%s", prefix, index, getCurrentTimeString());
    }

    /**
     * 生成大消息内容
     * @param prefix 消息前缀
     * @param sizeKB 消息大小（KB）
     * @return 大消息内容
     */
    public static String generateLargeMessage(String prefix, int sizeKB) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("_large_message_");
        
        // 填充内容到指定大小
        String padding = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        int targetSize = sizeKB * 1024;
        int currentSize = sb.length();
        
        while (currentSize < targetSize) {
            int remainingSize = targetSize - currentSize;
            if (remainingSize >= padding.length()) {
                sb.append(padding);
                currentSize += padding.length();
            } else {
                sb.append(padding, 0, remainingSize);
                currentSize = targetSize;
            }
        }
        
        return sb.toString();
    }

    /**
     * 获取当前时间字符串
     * @return 格式化的时间字符串
     */
    public static String getCurrentTimeString() {
        return LocalDateTime.now().format(FORMATTER);
    }

    /**
     * 获取当前时间戳（毫秒）
     * @return 时间戳
     */
    public static long getCurrentTimestamp() {
        return System.currentTimeMillis();
    }

    /**
     * 计算延迟时间差（毫秒）
     * @param expectedDelayMs 期望延迟时间
     * @param actualDelayMs 实际延迟时间
     * @return 时间差
     */
    public static long calculateDelayDifference(long expectedDelayMs, long actualDelayMs) {
        return Math.abs(actualDelayMs - expectedDelayMs);
    }

    /**
     * 验证延迟时间是否在容差范围内
     * @param expectedDelayMs 期望延迟时间
     * @param actualDelayMs 实际延迟时间
     * @param toleranceMs 容差时间（毫秒）
     * @return 是否在容差范围内
     */
    public static boolean isDelayWithinTolerance(long expectedDelayMs, long actualDelayMs, long toleranceMs) {
        long difference = calculateDelayDifference(expectedDelayMs, actualDelayMs);
        return difference <= toleranceMs;
    }

    /**
     * 等待指定时间
     * @param milliseconds 等待时间（毫秒）
     */
    public static void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Sleep interrupted", e);
        }
    }

    /**
     * 等待CountDownLatch完成
     * @param latch CountDownLatch
     * @param timeoutSeconds 超时时间（秒）
     * @return 是否在超时时间内完成
     */
    public static boolean awaitLatch(CountDownLatch latch, long timeoutSeconds) {
        try {
            return latch.await(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Latch await interrupted", e);
            return false;
        }
    }

    /**
     * 性能测试工具类
     */
    public static class PerformanceTest {
        private final StopWatch stopWatch;
        private final AtomicInteger successCount = new AtomicInteger(0);
        private final AtomicInteger failureCount = new AtomicInteger(0);
        private final AtomicLong totalResponseTime = new AtomicLong(0);
        private final String testName;

        public PerformanceTest(String testName) {
            this.testName = testName;
            this.stopWatch = new StopWatch(testName);
        }

        /**
         * 开始性能测试
         */
        public void start() {
            stopWatch.start();
            log.info("Performance test [{}] started", testName);
        }

        /**
         * 停止性能测试
         */
        public void stop() {
            if (stopWatch.isRunning()) {
                stopWatch.stop();
            }
            printResults();
        }

        /**
         * 记录成功操作
         * @param responseTimeMs 响应时间（毫秒）
         */
        public void recordSuccess(long responseTimeMs) {
            successCount.incrementAndGet();
            totalResponseTime.addAndGet(responseTimeMs);
        }

        /**
         * 记录失败操作
         */
        public void recordFailure() {
            failureCount.incrementAndGet();
        }

        /**
         * 执行操作并记录性能
         * @param operation 要执行的操作
         * @return 操作是否成功
         */
        public boolean executeAndRecord(Supplier<Boolean> operation) {
            long startTime = System.currentTimeMillis();
            try {
                boolean success = operation.get();
                long responseTime = System.currentTimeMillis() - startTime;
                if (success) {
                    recordSuccess(responseTime);
                } else {
                    recordFailure();
                }
                return success;
            } catch (Exception e) {
                long responseTime = System.currentTimeMillis() - startTime;
                recordFailure();
                log.error("Operation failed with response time: {}ms", responseTime, e);
                return false;
            }
        }

        /**
         * 打印测试结果
         */
        private void printResults() {
            int totalOperations = successCount.get() + failureCount.get();
            double successRate = totalOperations > 0 ? (double) successCount.get() / totalOperations * 100 : 0;
            double avgResponseTime = successCount.get() > 0 ? (double) totalResponseTime.get() / successCount.get() : 0;
            double tps = stopWatch.getTotalTimeSeconds() > 0 ? totalOperations / stopWatch.getTotalTimeSeconds() : 0;

            log.info("\n=== Performance Test Results [{}] ===\n" +
                    "Total Time: {:.2f}s\n" +
                    "Total Operations: {}\n" +
                    "Success Count: {}\n" +
                    "Failure Count: {}\n" +
                    "Success Rate: {:.2f}%\n" +
                    "Average Response Time: {:.2f}ms\n" +
                    "TPS (Transactions Per Second): {:.2f}\n" +
                    "==============================",
                    testName,
                    stopWatch.getTotalTimeSeconds(),
                    totalOperations,
                    successCount.get(),
                    failureCount.get(),
                    successRate,
                    avgResponseTime,
                    tps);
        }

        /**
         * 获取成功次数
         * @return 成功次数
         */
        public int getSuccessCount() {
            return successCount.get();
        }

        /**
         * 获取失败次数
         * @return 失败次数
         */
        public int getFailureCount() {
            return failureCount.get();
        }

        /**
         * 获取平均响应时间
         * @return 平均响应时间（毫秒）
         */
        public double getAverageResponseTime() {
            return successCount.get() > 0 ? (double) totalResponseTime.get() / successCount.get() : 0;
        }

        /**
         * 获取TPS
         * @return TPS值
         */
        public double getTps() {
            int totalOperations = successCount.get() + failureCount.get();
            return stopWatch.getTotalTimeSeconds() > 0 ? totalOperations / stopWatch.getTotalTimeSeconds() : 0;
        }
    }

    /**
     * 延迟消息测试工具类
     */
    public static class DelayMessageTest {
        private final long sendTime;
        private final long expectedDelayMs;
        private final String messageId;
        private long receiveTime;
        private boolean received = false;

        public DelayMessageTest(String messageId, long expectedDelayMs) {
            this.messageId = messageId;
            this.expectedDelayMs = expectedDelayMs;
            this.sendTime = System.currentTimeMillis();
        }

        /**
         * 标记消息已接收
         */
        public void markReceived() {
            this.receiveTime = System.currentTimeMillis();
            this.received = true;
        }

        /**
         * 获取实际延迟时间
         * @return 实际延迟时间（毫秒）
         */
        public long getActualDelayMs() {
            if (!received) {
                return -1;
            }
            return receiveTime - sendTime;
        }

        /**
         * 验证延迟时间是否准确
         * @param toleranceMs 容差时间（毫秒）
         * @return 是否准确
         */
        public boolean isDelayAccurate(long toleranceMs) {
            if (!received) {
                return false;
            }
            long actualDelay = getActualDelayMs();
            return isDelayWithinTolerance(expectedDelayMs, actualDelay, toleranceMs);
        }

        /**
         * 获取延迟差异
         * @return 延迟差异（毫秒）
         */
        public long getDelayDifference() {
            if (!received) {
                return -1;
            }
            return calculateDelayDifference(expectedDelayMs, getActualDelayMs());
        }

        /**
         * 打印延迟测试结果
         */
        public void printResult() {
            if (received) {
                long actualDelay = getActualDelayMs();
                long difference = getDelayDifference();
                log.info("Delay Message [{}] - Expected: {}ms, Actual: {}ms, Difference: {}ms",
                        messageId, expectedDelayMs, actualDelay, difference);
            } else {
                log.warn("Delay Message [{}] - Not received yet", messageId);
            }
        }

        // Getters
        public String getMessageId() { return messageId; }
        public long getExpectedDelayMs() { return expectedDelayMs; }
        public long getSendTime() { return sendTime; }
        public long getReceiveTime() { return receiveTime; }
        public boolean isReceived() { return received; }
    }
}