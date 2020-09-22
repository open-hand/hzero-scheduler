package org.hzero.scheduler.infra.redis;

import java.util.concurrent.TimeUnit;

import org.hzero.common.HZeroService;
import org.hzero.core.redis.RedisHelper;
import org.hzero.scheduler.config.SchedulerConfiguration;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import io.choerodon.core.convertor.ApplicationContextHelper;

/**
 * description
 *
 * @author shuangfei.zhu@hand-china.com 2019/06/20 10:35
 */
public class JobLock {

    private static RedisHelper redisHelper;
    private static RedissonClient redissonClient;

    private JobLock() {
    }

    private static RedisHelper getRedisHelper() {
        if (redisHelper == null) {
            redisHelper = ApplicationContextHelper.getContext().getBean(RedisHelper.class);
        }
        return redisHelper;
    }

    private static RedissonClient getRedisClient() {
        if (redissonClient == null) {
            redissonClient = ApplicationContextHelper.getContext().getBean(RedissonClient.class);
        }
        return redissonClient;
    }

    /**
     * 生成redis存储key
     *
     * @param jobId 任务ID
     * @return key
     */
    private static String getCacheKey(Long jobId) {
        return HZeroService.Scheduler.CODE + ":job-lock:" + jobId;
    }

    /**
     * 加锁
     *
     * @param jobId 任务Id
     */
    public static boolean addLock(Long jobId) {
        RLock fairLock = getRedisClient().getFairLock(getCacheKey(jobId));
        try {
            // 尝试加锁，最多等待0秒，上锁以后300秒自动解锁
            return fairLock.tryLock(0, ApplicationContextHelper.getContext().getBean(SchedulerConfiguration.class).getLockTime(), TimeUnit.SECONDS);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 清除锁
     *
     * @param jobId 任务Id
     */
    public static void clearLock(Long jobId) {
        getRedisHelper().delKey(getCacheKey(jobId));
    }
}
