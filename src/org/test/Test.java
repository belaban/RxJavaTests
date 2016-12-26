package org.test;

import rx.Observable;

public class Test {

    public static void main(String[] args) {

        Observable<String> obs=Observable.create(s -> {
            s.onNext("hello world");
            s.onNext("last one");
            s.onCompleted();
        });
        obs.subscribe(action -> System.out.printf("action: %s\n", action));
    }
}
