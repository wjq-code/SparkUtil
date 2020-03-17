package com.wjq.thread;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 多线程，线程池的使用
 */
public class ThreadPool {
//    private static ThreadPoolExecutor threadpool = new ThreadPoolExecutor(5, 5,
//            0L, TimeUnit.MILLISECONDS,
//            new BlockingQueue<Runnable>(5),(r,executor) -> {
//        try {
//            // 线程阻塞
//            executor.execute(executor.getQueue().take());
//            executor.getQueue().put(r);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }printStackTrace
//    });
}
