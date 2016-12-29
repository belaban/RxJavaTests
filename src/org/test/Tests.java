package org.test;

import org.jgroups.util.Tuple;
import org.jgroups.util.Util;
import org.testng.annotations.Test;
import rx.Completable;
import rx.Observable;
import rx.Single;
import rx.Subscriber;
import rx.functions.Func1;
import rx.observables.ConnectableObservable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static rx.Observable.just;
import static rx.Observable.timer;

@Test(singleThreaded=true)
public class Tests {
    protected static final long START=System.currentTimeMillis();

    public void merge() {
        Observable<Integer> obs1=Observable.range(1, 5).delay(300, MILLISECONDS)
          .doOnNext(i -> log(String.valueOf(i)));

        Observable<Integer> obs2=Observable.range(6, 10).delay(200, MILLISECONDS)
          .doOnNext(i -> log(String.valueOf(i)));

        Observable.merge(obs1, obs2).subscribe(s -> log("main: " + s));

        Util.sleep(2000);
    }

    public void observeOn() {
        Observable.create(s -> {
            s.onNext("one");
            s.onNext("two");
            s.onCompleted();
        }).observeOn(Schedulers.io())
          .subscribe(s -> log((String)s));
    }

    public void testPublishSubject() {
        PublishSubject<Integer> subject=PublishSubject.create();

        subject.onNext(1);

        subject.subscribe(i -> log("i=" +i));

        for(int i=2; i <= 5; i++)
            subject.onNext(i);
    }

    public void flatMapConcurrent() {
        Observable<String> obs1=Observable.defer(() -> Observable.just(findFlight()));
        obs1.flatMap(flight -> Observable.fromCallable(() -> new Tuple(flight, findCar())).subscribeOn(Schedulers.io()))
          .subscribe(booking -> System.out.printf("booking: %s (class: %s)\n", booking, booking.getClass().getSimpleName()));
    }

    public void zipConcurrent() {
        Observable<String> obs1=Observable.fromCallable(this::findFlight);
        Observable<String> obs2=Observable.fromCallable(this::findCar);

        Observable.zip(obs1.subscribeOn(Schedulers.io()),
                       obs2.subscribeOn(Schedulers.io()),
                       (flight,car) -> String.format("flight: %s car: %s", flight, car))
          .subscribe(booking -> log(String.format("-- booking: %s\n", booking)));

        Util.sleep(3000);

    }

    protected String findFlight() {
        log("finding flight");
        Util.sleep(2000);
        return "LX 59";
    }

    protected String findCar() {
        log("finding car");
        Util.sleep(1000);
        return "Ferrari";
    }


    protected void log(String s) {
        System.out.printf("| %5d | [%s] %s\n", System.currentTimeMillis() - START, Thread.currentThread().getName(), s);
    }

    public void defer() {
        Observable<List<String>> obs=Observable.defer(() -> Observable.just(findPeople()));

        obs.flatMap(Observable::from).toBlocking().subscribe(p -> System.out.println("p = " + p));
    }

    protected List<String> findPeople() {
        System.out.printf("-- finding people:\n");
        return Arrays.asList("Bela", "Jeannette", "Michelle", "Nicole");
    }


    public void lift() {
        Observable<Integer> obs=Observable.just("one", "two", "three", "four")
          .lift(new Mapper<>(String::length));
        //.lift(new MyMapper());

        obs.subscribe(s -> System.out.println("s = " + s));
    }

    protected static class Mapper<T,R> implements Observable.Operator<R,T> {
        protected final Func1<T,R> map_func;

        public Mapper(Func1<T,R> map_func) {
            this.map_func=map_func;
        }

        public Subscriber<? super T> call(Subscriber<? super R> child) {
            return new Subscriber<T>() {
                public void onCompleted() {
                    child.onCompleted();
                }

                public void onError(Throwable e) {
                    child.onError(e);
                }

                public void onNext(T t) {
                    try {
                        child.onNext(map_func.call(t));
                    }
                    catch(Exception ex) {
                        child.onError(ex);
                    }
                }
            };
        }
    }


    protected static class MyMapper implements Observable.Operator<Integer,String> {

        public Subscriber<? super String> call(Subscriber<? super Integer> child) {
            return new Subscriber<String>() {
                public void onCompleted() {
                    child.onCompleted();
                }

                public void onError(Throwable e) {
                    child.onError(e);
                }

                public void onNext(String s) {
                    child.onNext(s.length());
                }
            };
        }
    }


    public void compose() {


        Observable<Integer> obs=Observable.just("one", "two", "three", "four")
          .compose(length());

        obs.subscribe(i -> System.out.println("i = " + i));
    }


    static Observable.Transformer<String,Integer> length() {
        return upstream -> upstream.doOnNext(s -> System.out.printf("%s:\n", s)).map(String::length);
    }

    protected static class MyCount<U,D> implements Observable.Transformer<U,D> {

        public Observable<D> call(Observable<U> uObservable) {
            return null;
        }
    }


    public void timestamp() {
        long start=System.currentTimeMillis();

        Observable.interval(10, MILLISECONDS)
          .timestamp()
          .zipWith(Observable.interval(11, MILLISECONDS)
          .timestamp(), (ts1, ts2) -> ts2.getTimestampMillis() - ts1.getTimestampMillis())
          .takeWhile(ignore -> System.currentTimeMillis() - start < 1000).toBlocking()
          .subscribe(System.out::println);

        long time=System.currentTimeMillis() - start;
        System.out.println("time = " + time);
    }

    public void zip() {
        Observable<Integer>obs1=Observable.just(1, 2, 3, 4, 5);
        Observable<String> obs2=Observable.just("A", "B", "C", "D", "E", "F");

        Observable.zip(obs1,obs2, (n,s) -> String.format("%d%s", n,s))
          .subscribe(el -> System.out.printf("%s\n", el));
    }

    public void multipleObservablesAndErrors() {
        Observable<Integer>[] observables=new Observable[3];
        for(int i=0; i < observables.length; i++) {
            observables[i]=Observable.create(s -> {
                IntStream.rangeClosed(1,10).forEach(num -> {
                    if(Util.tossWeightedCoin(.2))
                        s.onError(new RuntimeException("error-" + (num+1)));
                    else
                        s.onNext(num);
                });
            });
        }


        Observable<Integer> obs=Observable.mergeDelayError(Arrays.asList(observables));
        obs.subscribe(n -> System.out.printf("n: %d\n", n),
                      ex -> System.err.printf("ex: %s", ex));
    }

    public void multipleObservables() {
        Observable<Integer> o1=Observable.just(1,2,3,4,5)
          .delay(num -> timer(num*100, MILLISECONDS));

        Observable<Integer> o2=Observable.just(1,2,3,4,5)
                  .delay(num -> timer(num*200, MILLISECONDS));

        Observable<Integer> o3=Observable.just(1,2,3,4,5)
                  .delay(num -> timer(num*500, MILLISECONDS));

        Observable.merge(o1, o2, o3)
          .distinct()
          .subscribe(num -> System.out.printf("%d\n", num));

        Util.sleep(5000);
    }


    public void concatMap() {
        Observable<Long> obs=Observable.just(5L, 1L, 3L, 2L)
          .concatMap(n -> just(n).delay(n, SECONDS));

        obs.subscribe(System.out::println);

        Util.sleep(7000);
    }

    public void delay2() {
        Observable<Integer> obs=just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
          .doOnSubscribe(() -> System.out.println("subscribe()"))
          .doOnNext(el -> System.out.printf("el=%s\n", el));

        obs.subscribe(s -> System.out.printf("%s\n", s));
    }

    public void delay() {
        Observable<Integer> obs=Observable.create(s -> IntStream.rangeClosed(1, 10)
          .forEach(s::onNext));
        obs=obs
          .delay(num -> {
              Util.sleep(500);
              return just(num);
          });


        obs.subscribeOn(Schedulers.newThread()).subscribe(num -> System.out.printf("[%s] %d\n", Thread.currentThread().getName(), num));
        obs.subscribe(num -> System.out.printf("[%s] %d\n", Thread.currentThread().getName(), num));

    }

    public void flatMap() {

        List<List<String>> lists=Arrays.asList(Arrays.asList("A", "B", "C"), Arrays.asList("M", "N", "O"),
                                               Arrays.asList("X", "Y", "Z"));

        // Observable<List<String>> obs=Observable.just(Arrays.asList("A", "B", "C"), Arrays.asList("M", "N", "O"),
           //                                           Arrays.asList("X", "Y", "Z"));
        Observable<List<String>> obs=Observable.from(lists);
        Observable<String> o=obs.flatMapIterable(l -> l);

        o.subscribe(str -> System.out.println("str = " + str));

    }

    public void connectable() {
        ConnectableObservable<Integer> obs=Observable.<Integer> create(s -> IntStream.rangeClosed(1, 10)
          .forEach(s::onNext)).publish();

        obs.subscribe(num -> System.out.printf("[%s] %d\n", Thread.currentThread().getName(), num));
        obs.subscribe(num -> System.out.printf("[%s] %d\n", Thread.currentThread().getName(), num));

        Util.sleep(2000);
        obs.connect();

    }

   /* public void twitterSearch() throws TwitterException {
        // The factory instance is re-useable and thread safe.
        Twitter twitter = TwitterFactory.getSingleton();
        Query query = new Query("source:twitter4j yusukey");
        QueryResult result = twitter.search(query);
        for (Status status : result.getTweets()) {
            System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());
        }

    }*/



    public void cache() {
        Observable<Integer> obs=Observable.create(s -> IntStream.rangeClosed(1, 10).forEach(s::onNext));

        obs=obs.cache();

        obs.subscribe(num -> System.out.printf("[%s] %d\n", Thread.currentThread().getName(), num));

        obs.subscribe(num -> System.out.printf("[%s] %d\n", Thread.currentThread().getName(), num));
    }

    public void error() {
        Observable.<Integer>error(new NullPointerException("booo"))
          .subscribe(System.out::println,
                     Throwable::printStackTrace);
    }

    public void simple() {
        Observable.<Integer>create(s -> {
            IntStream.rangeClosed(1,10).forEach(s::onNext);
            // s.onCompleted();
            s.onError(new NullPointerException("boom"));
        }).subscribe(System.out::println,
                     Throwable::printStackTrace,
                     () -> System.out.println("done"));
    }


    public void testUnsubscribe() {
        Observable<String> good=Observable.create(s -> {
            IntStream.rangeClosed(1, 50).forEach(num -> s.onNext("hello-" + num));
            s.onCompleted();
        });

        Subscriber<String> ss=new Subscriber<String>() {
            int items=0;

            public void onCompleted() {}
            public void onError(Throwable e) {}

            public void onNext(String s) {
                System.out.printf("%s\n", s);
                if(++items >= 3)
                    unsubscribe();
            }
        };

        good.skip(10).take(5).map(s -> s + " (done)").subscribe(ss);
    }

    public void single() {
        Single<String> a=Single.<String>create(o -> o.onSuccess("A")).subscribeOn(Schedulers.io());
        Single<String> b=Single.<String>create(o -> o.onSuccess("B")).subscribeOn(Schedulers.io());

        Single.merge(a,b).subscribe(s -> System.out.printf("%s", s));
    }

    public void observable() {
        Completable comp=Completable.create(s -> {
            Util.sleep(1000);
            s.onError(new NullPointerException("booom"));
        });

        comp.subscribe(() -> System.out.printf("done\n"),
                       ex -> System.out.printf("exception: %s\n", ex));
    }

    public void interval() {
        Subscriber<Long> sub=new Subscriber<Long>() {
            public void onCompleted() {}
            public void onError(Throwable e) {}
            public void onNext(Long aLong) {
                System.out.println("aLong = " + aLong);
            }
        };


        TestSubscriber<Long> ts=new TestSubscriber(sub);


        Observable.interval(1000, TimeUnit.MILLISECONDS)
          .limit(10)
          .timeout(5000, TimeUnit.MILLISECONDS)
          .subscribe(ts);

        ts.awaitTerminalEvent();
    }



}