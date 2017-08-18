package com.netflix.hystrix.util;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.cloud.sleuth.DefaultSpanNamer;
import org.springframework.cloud.sleuth.SpanNamer;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.sleuth.instrument.async.LazyTraceThreadPoolTaskExecutor;
import org.springframework.cloud.sleuth.instrument.async.LocalComponentTraceCallable;
import org.springframework.cloud.sleuth.instrument.async.LocalComponentTraceRunnable;
import org.springframework.cloud.sleuth.instrument.async.TraceableScheduledExecutorService;

/**
 * @author Marcin Grzejszczak
 */
public class HystrixTimerHacker {

	private final BeanFactory beanFactory;
	private Tracer tracer;
	private TraceKeys traceKeys;
	private SpanNamer spanNamer;

	public HystrixTimerHacker(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

	public void hackHystrixTimer() {
		HystrixTimer timer = HystrixTimer.getInstance();
		timer.startThreadIfNeeded();
		HystrixTimer.ScheduledExecutor executor = timer.executor.get();
		ScheduledThreadPoolExecutor threadPoolExecutor = executor.executor;
		executor.executor = new TraceScheduledThreadPoolExecutor(threadPoolExecutor, tracer(), traceKeys(), spanNamer(), beanFactory);
	}

	private Tracer tracer() {
		if (this.tracer == null) {
			this.tracer = this.beanFactory.getBean(Tracer.class);
		}
		return this.tracer;
	}

	private TraceKeys traceKeys() {
		if (this.traceKeys == null) {
			try {
				this.traceKeys = this.beanFactory.getBean(TraceKeys.class);
			}
			catch (NoSuchBeanDefinitionException e) {
				return new TraceKeys();
			}
		}
		return this.traceKeys;
	}

	private SpanNamer spanNamer() {
		if (this.spanNamer == null) {
			try {
				this.spanNamer = this.beanFactory.getBean(SpanNamer.class);
			}
			catch (NoSuchBeanDefinitionException e) {
				return new DefaultSpanNamer();
			}
		}
		return this.spanNamer;
	}
}


class TraceScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

	private final TraceableScheduledExecutorService service;
	private final LazyTraceThreadPoolExecutor executor;

	public TraceScheduledThreadPoolExecutor(ScheduledThreadPoolExecutor delegate,
			Tracer tracer, TraceKeys traceKeys, SpanNamer spanNamer, BeanFactory beanFactory) {
		super(delegate.getCorePoolSize(), delegate.getThreadFactory(), delegate.getRejectedExecutionHandler());
		this.service = new TraceableScheduledExecutorService(delegate, tracer, traceKeys, spanNamer);
		this.executor = new LazyTraceThreadPoolExecutor(delegate, tracer, traceKeys, spanNamer);
	}

	@Override public ScheduledFuture<?> schedule(Runnable command, long delay,
			TimeUnit unit) {
		return service.schedule(command, delay, unit);
	}

	@Override public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay,
			TimeUnit unit) {
		return service.schedule(callable, delay, unit);
	}

	@Override public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
			long initialDelay, long period, TimeUnit unit) {
		return service.scheduleAtFixedRate(command, initialDelay, period, unit);
	}

	@Override public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
			long initialDelay, long delay, TimeUnit unit) {
		return service.scheduleWithFixedDelay(command, initialDelay, delay, unit);
	}

	@Override public void execute(Runnable command) {
		service.execute(command);
	}

	@Override public void shutdown() {
		service.shutdown();
	}

	@Override public List<Runnable> shutdownNow() {
		return service.shutdownNow();
	}

	@Override public boolean isShutdown() {
		return service.isShutdown();
	}

	@Override public boolean isTerminating() {
		return executor.isTerminating();
	}

	@Override public boolean isTerminated() {
		return service.isTerminated();
	}

	@Override public boolean awaitTermination(long timeout, TimeUnit unit)
			throws InterruptedException {
		return service.awaitTermination(timeout, unit);
	}

	@Override public void setThreadFactory(ThreadFactory threadFactory) {
		executor.setThreadFactory(threadFactory);
	}

	@Override public ThreadFactory getThreadFactory() {
		return executor.getThreadFactory();
	}

	@Override public void setRejectedExecutionHandler(RejectedExecutionHandler handler) {
		executor.setRejectedExecutionHandler(handler);
	}

	@Override public RejectedExecutionHandler getRejectedExecutionHandler() {
		return executor.getRejectedExecutionHandler();
	}

	@Override public void setCorePoolSize(int corePoolSize) {
		executor.setCorePoolSize(corePoolSize);
	}

	@Override public int getCorePoolSize() {
		return executor.getCorePoolSize();
	}

	@Override public boolean prestartCoreThread() {
		return executor.prestartCoreThread();
	}

	@Override public int prestartAllCoreThreads() {
		return executor.prestartAllCoreThreads();
	}

	@Override public boolean allowsCoreThreadTimeOut() {
		return executor.allowsCoreThreadTimeOut();
	}

	@Override public void allowCoreThreadTimeOut(boolean value) {
		executor.allowCoreThreadTimeOut(value);
	}

	@Override public void setMaximumPoolSize(int maximumPoolSize) {
		executor.setMaximumPoolSize(maximumPoolSize);
	}

	@Override public int getMaximumPoolSize() {
		return executor.getMaximumPoolSize();
	}

	@Override public void setKeepAliveTime(long time, TimeUnit unit) {
		executor.setKeepAliveTime(time, unit);
	}

	@Override public long getKeepAliveTime(TimeUnit unit) {
		return executor.getKeepAliveTime(unit);
	}

	@Override public BlockingQueue<Runnable> getQueue() {
		return executor.getQueue();
	}

	@Override public boolean remove(Runnable task) {
		return executor.remove(task);
	}

	@Override public void purge() {
		executor.purge();
	}

	@Override public int getPoolSize() {
		return executor.getPoolSize();
	}

	@Override public int getActiveCount() {
		return executor.getActiveCount();
	}

	@Override public int getLargestPoolSize() {
		return executor.getLargestPoolSize();
	}

	@Override public long getTaskCount() {
		return executor.getTaskCount();
	}

	@Override public long getCompletedTaskCount() {
		return executor.getCompletedTaskCount();
	}

	@Override public String toString() {
		return executor.toString();
	}

	@Override public <T> Future<T> submit(Callable<T> task) {
		return service.submit(task);
	}

	@Override public <T> Future<T> submit(Runnable task, T result) {
		return service.submit(task, result);
	}

	@Override public Future<?> submit(Runnable task) {
		return service.submit(task);
	}

	@Override public <T> List<Future<T>> invokeAll(
			Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return service.invokeAll(tasks);
	}

	@Override public <T> List<Future<T>> invokeAll(
			Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
		return service.invokeAll(tasks, timeout, unit);
	}

	@Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
			throws InterruptedException, ExecutionException {
		return service.invokeAny(tasks);
	}

	@Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
			long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return service.invokeAny(tasks, timeout, unit);
	}

}

class LazyTraceThreadPoolExecutor extends ThreadPoolExecutor {

	private final ThreadPoolExecutor delegate;
	private final Tracer tracer;
	private final TraceKeys traceKeys;
	private final SpanNamer spanNamer;

	public LazyTraceThreadPoolExecutor(ThreadPoolExecutor delegate, Tracer tracer,
			TraceKeys traceKeys, SpanNamer spanNamer) {
		super(delegate.getCorePoolSize(), delegate.getMaximumPoolSize(), delegate.getKeepAliveTime(TimeUnit.NANOSECONDS),
				TimeUnit.NANOSECONDS, delegate.getQueue(), delegate.getThreadFactory(), delegate.getRejectedExecutionHandler());
		this.delegate = delegate;
		this.tracer = tracer;
		this.traceKeys = traceKeys;
		this.spanNamer = spanNamer;
	}

	@Override public void execute(Runnable command) {
		delegate.execute(new LocalComponentTraceRunnable(tracer, traceKeys, spanNamer, command));
	}

	@Override public void shutdown() {
		delegate.shutdown();
	}

	@Override public List<Runnable> shutdownNow() {
		return delegate.shutdownNow();
	}

	@Override public boolean isShutdown() {
		return delegate.isShutdown();
	}

	@Override public boolean isTerminating() {
		return delegate.isTerminating();
	}

	@Override public boolean isTerminated() {
		return delegate.isTerminated();
	}

	@Override public boolean awaitTermination(long timeout, TimeUnit unit)
			throws InterruptedException {
		return delegate.awaitTermination(timeout, unit);
	}

	@Override public void setThreadFactory(ThreadFactory threadFactory) {
		delegate.setThreadFactory(new ThreadFactory() {
			@Override public Thread newThread(Runnable r) {
				return threadFactory.newThread(new LocalComponentTraceRunnable(tracer, traceKeys, spanNamer, r));
			}
		});
	}

	@Override public ThreadFactory getThreadFactory() {
		return delegate.getThreadFactory();
	}

	@Override public void setRejectedExecutionHandler(RejectedExecutionHandler handler) {
		delegate.setRejectedExecutionHandler(handler);
	}

	@Override public RejectedExecutionHandler getRejectedExecutionHandler() {
		return delegate.getRejectedExecutionHandler();
	}

	@Override public void setCorePoolSize(int corePoolSize) {
		delegate.setCorePoolSize(corePoolSize);
	}

	@Override public int getCorePoolSize() {
		return delegate.getCorePoolSize();
	}

	@Override public boolean prestartCoreThread() {
		return delegate.prestartCoreThread();
	}

	@Override public int prestartAllCoreThreads() {
		return delegate.prestartAllCoreThreads();
	}

	@Override public boolean allowsCoreThreadTimeOut() {
		return delegate.allowsCoreThreadTimeOut();
	}

	@Override public void allowCoreThreadTimeOut(boolean value) {
		delegate.allowCoreThreadTimeOut(value);
	}

	@Override public void setMaximumPoolSize(int maximumPoolSize) {
		delegate.setMaximumPoolSize(maximumPoolSize);
	}

	@Override public int getMaximumPoolSize() {
		return delegate.getMaximumPoolSize();
	}

	@Override public void setKeepAliveTime(long time, TimeUnit unit) {
		delegate.setKeepAliveTime(time, unit);
	}

	@Override public long getKeepAliveTime(TimeUnit unit) {
		return delegate.getKeepAliveTime(unit);
	}

	@Override public BlockingQueue<Runnable> getQueue() {
		return delegate.getQueue();
	}

	@Override public boolean remove(Runnable task) {
		return delegate.remove(new LocalComponentTraceRunnable(tracer, traceKeys, spanNamer, task));
	}

	@Override public void purge() {
		delegate.purge();
	}

	@Override public int getPoolSize() {
		return delegate.getPoolSize();
	}

	@Override public int getActiveCount() {
		return delegate.getActiveCount();
	}

	@Override public int getLargestPoolSize() {
		return delegate.getLargestPoolSize();
	}

	@Override public long getTaskCount() {
		return delegate.getTaskCount();
	}

	@Override public long getCompletedTaskCount() {
		return delegate.getCompletedTaskCount();
	}

	@Override public String toString() {
		return delegate.toString();
	}

	@Override public Future<?> submit(Runnable task) {
		return delegate.submit(new LocalComponentTraceRunnable(tracer, traceKeys, spanNamer, task));
	}

	@Override public <T> Future<T> submit(Runnable task, T result) {
		return delegate.submit(new LocalComponentTraceRunnable(tracer, traceKeys, spanNamer, task), result);
	}

	@Override public <T> Future<T> submit(Callable<T> task) {
		return delegate.submit(new LocalComponentTraceCallable<T>(tracer, traceKeys, spanNamer, task));
	}

	@Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
			throws InterruptedException, ExecutionException {
		return delegate.invokeAny(tasks);
	}

	@Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
			long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return delegate.invokeAny(tasks, timeout, unit);
	}

	@Override public <T> List<Future<T>> invokeAll(
			Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return delegate.invokeAll(tasks);
	}

	@Override public <T> List<Future<T>> invokeAll(
			Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
		return delegate.invokeAll(tasks, timeout, unit);
	}
}