package org.example;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.parallel.ParallelFlowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subscribers.DisposableSubscriber;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

public class Main {
    public static void main(String[] args) throws IOException {
        List<Integer> l = new LinkedList<>();
        for(int i = 0; i < 30; i++)
            l.add(i);
        //sequential(l);
        parallel(l);

        // Because subscribe() is asynchronous
        System.in.read();
    }

    public static void sequential(List<Integer> l) {
        Flowable.fromIterable(l)
                .subscribeOn(Schedulers.io())
                .forEach(i -> {
                    System.out.println(Instant.now() + " - Processing " + i);
                    Thread.sleep(1000);
                });
    }

    public static void parallel(List<Integer> l) {
        ParallelFlowable<Integer> f = Flowable.fromIterable(l)
                .parallel(30)
                .runOn(Schedulers.io());

        class Processor extends DisposableSubscriber<Integer> {
            @Override
            public void onNext(Integer integer) {
                System.out.println(Instant.now() + " - Thread " + Thread.currentThread().getId() + " - Processing " + integer);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println("Error happened: " + throwable.getMessage());
            }

            @Override
            public void onComplete() {
                System.out.println(Thread.currentThread().getId() + " Completed");
            }
        }

        Processor[] processors = Stream.generate(Processor::new).limit(f.parallelism()).toArray(Processor[]::new);
        f.subscribe(processors);

    }
}