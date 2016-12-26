package org.test;

import org.jgroups.util.Util;
import rx.Observable;

public class Test {

    public static void main(String[] args) {

        Observable<Integer> obs1=Observable.create(s -> {
            new Thread(() -> {
                for(int i=1; i <= 10; i++) {
                    if(i % 2 == 0)
                        s.onNext(i);
                    Util.sleep(500);
                }
                s.onCompleted();
            }).start();

        });

        Observable<Integer> obs2=Observable.create(s -> {
            new Thread(() -> {
                for(int i=1; i <= 15; i++) {
                    if(i % 2 != 0)
                        s.onNext(i);
                    Util.sleep(500);
                }
                s.onCompleted();
            }).start();

        });

        Observable.merge(obs1, obs1).subscribe(i -> System.out.printf("[%d] %d\n", Thread.currentThread().getId(), i));
    }
}
