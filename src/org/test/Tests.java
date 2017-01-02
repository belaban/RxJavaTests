package org.test;


import org.jgroups.util.Tuple;
import org.jgroups.util.Util;
import org.testng.annotations.Test;
import rx.*;
import rx.functions.Func1;
import rx.observables.ConnectableObservable;
import rx.observables.GroupedObservable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static rx.Observable.just;
import static rx.Observable.timer;

@Test(singleThreaded=true)
public class Tests {
    protected static final long START=System.currentTimeMillis();

    public void toBlocking() {
        List<Integer> list=Observable.range(1, 10)
          .toList()
          .toBlocking()
          .single();
        System.out.println("list = " + list);
    }


    public void backPressure4() {
        Observable<Integer> obs=Observable.create(new MyRange(1, 250));
        obs.subscribe(new Subscriber<Integer>() {

            public void onStart() {
                request(3);
            }

            public void onCompleted() {

            }

            public void onError(Throwable e) {

            }

            public void onNext(Integer integer) {
                System.out.println("integer = " + integer);
                request(1);
            }
        });
    }


    protected static class MyRange implements Observable.OnSubscribe<Integer> {
        protected final int from , to;


        public MyRange(int from, int to) {
            this.from=from;
            this.to=to;
        }

        public void call(Subscriber<? super Integer> child) {
            child.setProducer(new MyRangeImpl(child, from, to));
        }
    }

    protected static class MyRangeImpl implements Producer {
        protected Subscriber<? super Integer> sub;
        protected int from, to;
        protected boolean done, processing;
        protected int requested=0;

        public MyRangeImpl(Subscriber<? super Integer> subscriber, int from, int to) {
            this.sub=subscriber;
            this.from=from;
            this.to=to;
        }

        public void request(long n) {
            requested+=n;
            System.out.printf("requested %d, requests left: %d\n", n, requested);
            if(processing)
                return;

            processing=true;
            while(requested > 0 && from <= to) {
                requested--;
                sub.onNext(from++);
            }
            processing=false;
            if(from >= to && !done) {
                done=true;
                sub.onCompleted();
            }
        }
    }


    public void backPressure3() {
        Observable<Dish> dishes=Observable
          .range(1, 1_000)
          /*.<Integer>create(s -> {
              for(int i=0; i < 1000; i++) {
                  s.onNext(i);
              }
              s.onCompleted();
          })*/
          .map(Dish::new);


          dishes
            .observeOn(Schedulers.io())
            .subscribe(new Subscriber<Dish>() {
                public void onStart() {
                    request(3);
                }

                public void onCompleted() {
                }

                public void onError(Throwable e) {
                    e.printStackTrace(System.err);
                }

                public void onNext(Dish d) {
                    log("washing " + d.id);
                    Util.sleep(50);
                    request(1);
                }
            });



        Util.sleep(10000);
    }


    protected class Dish {
        protected final int    id;
        protected final byte[] buf=new byte[1024];

        public Dish(int id) {
            this.id=id;
            log("created " + id);
        }

        public String toString() {
            return String.valueOf(id);
        }
    }

    public void fromEmitter() {
        Observable<Integer> obs=Observable.fromEmitter(emitter -> {
            for(int i=1; i <= 100; i++) {
                emitter.onNext(i);
                log(String.format("i=%d, requested=%d", i, emitter.requested()));
            }
            emitter.onCompleted();
        }, Emitter.BackpressureMode.BUFFER);


        obs
          .observeOn(Schedulers.io())
          .subscribeOn(Schedulers.io())
          .subscribe(new Subscriber<Integer>() {
              Producer prod;

            public void onStart() {
                request(1);
            }

            public void onCompleted() {

            }

            public void onError(Throwable e) {
                System.err.printf("error: %s", e);
            }

            public void onNext(Integer integer) {
                log("integer = " + integer);
                Util.sleep(100);
                request(1);
            }

          });

        Util.sleep(5000);
    }


    public void backPressure2() {
        Observable<Integer> obs=Observable.create(s -> {
            new Thread(() -> {
                for(int i=1; i <= 100; i++) {
                    s.onNext(i);
                }
                s.onCompleted();
                log("observable is done");
            }).start();
        });

        obs
          .onBackpressureBuffer(10, () -> System.err.printf("bad: overflow!!!"))
          .subscribeOn(Schedulers.io())
          .subscribe(new Subscriber<Integer>() {

            public void onStart() {
                request(1);
            }

            public void onCompleted() {

            }

            public void onError(Throwable e) {
                System.err.printf("error: %s", e);
            }

            public void onNext(Integer integer) {
                log("integer = " + integer);
                Util.sleep(100);
                request(1);
            }
        });

        Util.sleep(5000);
    }


    protected class MyObservable<T> extends Observable<T> {
        protected final int from, to;

        /**
         * Creates an Observable with a Function to execute when it is subscribed to.
         * <p>
         * <em>Note:</em> Use {@link #create(OnSubscribe)} to create an Observable, instead of this constructor,
         * unless you specifically have a need for inheritance.
         * @param f {@link OnSubscribe} to be executed when {@link #subscribe(Subscriber)} is called
         * @param from
         * @param to
         */
        protected MyObservable(OnSubscribe<T> f, int from, int to) {
            super(f);
            this.from=from;
            this.to=to;
        }




    }



    public void window() {
        Observable<Integer> obs=Observable.create(s -> {
            for(int i=1; i <= 10; i++) {
                s.onNext(i);
                Util.sleep(100);
            }
            s.onCompleted();
        });
        obs.window(Observable.empty(),
                   num -> Observable.empty().delay(500, MILLISECONDS))
          .flatMap(m -> m)
          .subscribe(System.out::println);


        Util.sleep(2000);
    }

    public void throttleWithTimeout() {
        Observable<Integer> obs=Observable.create(s -> {
            for(int i=1; i <= 10; i++) {
                s.onNext(i);
                Util.sleep(800);
            }
            s.onCompleted();
        });

        obs.throttleFirst(2, SECONDS)
          .subscribe(System.out::println);

        //obs.throttleWithTimeout(1, SECONDS)
          //.subscribe(System.out::println);
    }


    public void backPressure() throws Exception {
        final byte[] buf=new byte[10000];
        final CountDownLatch latch=new CountDownLatch(1);
        final AtomicInteger  count=new AtomicInteger(0);
        final AtomicLong     bytes=new AtomicLong(0);

        Observable.<byte[]>create(s -> {
            for(int i=0; i < 1_000_000; i++)
                s.onNext(buf);
            log("observable is done");
            s.onCompleted();
            latch.countDown();
        })
          .observeOn(Schedulers.io())
          .subscribe(num -> {
                         // create some delay in consuming the stream
                         Util.sleep(1);
                         if(count.incrementAndGet() % 1000 == 0)
                             log(String.format("count: %d, bytes: %s\n", count.get(), Util.printBytes(bytes.get())));
                         bytes.addAndGet(num.length);
                     },
                     ex -> System.err.printf("exception: %s\n", ex.toString())
          );

        latch.await();
        System.out.printf("\n\ncount=%d, bytes=%s\n", count.get(), Util.printBytes(bytes.get()));

    }

    public void single2() {
        Single<Integer> single=Single.just(100);
        System.out.println("single = " + single);
        single.subscribe(num -> System.out.println("num = " + num));
    }

    public void completableFutureToObserver() {
        Observable<String> obs=Observable.defer(() ->
                                                  from(CompletableFuture.supplyAsync(() -> {
                                                      log("sleeping for 1 s");
                                                      Util.sleep(1000);
                                                      return "hello world";
                                                  })));

        Util.sleep(500);
        log("subscribing");
        obs.subscribe(s -> log("s = " + s));

        Util.sleep(2000);
    }

    protected static <T> Observable<T> from(CompletableFuture<T> future) {
        return Observable.create(s -> future.whenComplete((r, ex) -> {
            if(ex != null)
                s.onError(ex);
            else {
                s.onNext(r);
                s.onCompleted();
            }
        }));
    }


    public void from() {

        String[] strings={"hello", "world", "from", "bela"};

        Observable<String> obs=Observable.from(strings);

        obs.subscribe(System.out::println);
    }

    public void wordCount() throws Exception {
        // URL url=new URL("http://www.gutenberg.org/ebooks/218.txt.utf-8");
        Observable<String> obs=Observable.create(s -> {
            BufferedReader in=null;
            try {
                in=new BufferedReader(new InputStreamReader(new FileInputStream("/home/bela/DeBelloGallicum.txt")));
                String inputLine;
                while((inputLine=in.readLine()) != null)
                    if(!inputLine.isEmpty())
                        s.onNext(inputLine);
                s.onCompleted();
            }
            catch(Throwable ex) {
                s.onError(ex);
            }
            finally {
                Util.close(in);
            }
        });

        /*Observable<Map<String,Integer>> obs2=obs
          .flatMap(line -> Observable.from(line.split("\\s+")))
          .filter(s -> s != null && !s.isEmpty())
          .reduce(new HashMap<>(),
                  (map,key) -> {
                      Integer val=map.get(key);
                      if(val == null)
                          map.put(key, 1);
                      else
                          map.put(key, val +1);
                      return map;
                  });

        obs2.subscribe(map -> {
            for(Map.Entry<String,Integer> entry: map.entrySet())
                System.out.printf("%s: %d\n", entry.getKey(), entry.getValue());
        });
*/
        obs
          .flatMap(line -> Observable.from(line.split("\\s+")))
          .groupBy(String::toLowerCase)
          .flatMap(this::count)
          .filter(cnt -> cnt.num > 0)
          .toSortedList()
          .subscribe(System.out::println);

        //Util.sleep(3000);
    }

    private Observable<Count> count(GroupedObservable<String, String> group) {
        return group.scan(new Count(group.getKey()),
                          (count, string) -> count.incr()
        );
    }

    protected class Count implements Comparable<Count> {
        protected final String key;
        protected int num;

        public Count(String key) {
            this.key=key;
        }

        public Count incr() {num++; return this;}

        public String toString() {
            return String.format("%s: %d", key, num);
        }

        public int compareTo(Count o) {
            return Integer.compare(num, o.num);
        }
    }


    public void groupBy() {
        Observable.range(1, 100).map(i -> Util.random(20))
          .groupBy(g -> g)
          .subscribe(el -> System.out.println("el = " + el.getKey()));
    }


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
        Observable<String> obs1=Observable.defer(() -> just(findFlight()));
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
        Observable<List<String>> obs=Observable.defer(() -> just(findPeople()));

        obs.flatMap(Observable::from).toBlocking().subscribe(p -> System.out.println("p = " + p));
    }

    protected List<String> findPeople() {
        System.out.printf("-- finding people:\n");
        return Arrays.asList("Bela", "Jeannette", "Michelle", "Nicole");
    }


    public void lift() {
        Observable<Integer> obs=just("one", "two", "three", "four")
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


        Observable<Integer> obs=just("one", "two", "three", "four")
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
        Observable<Integer>obs1=just(1, 2, 3, 4, 5);
        Observable<String> obs2=just("A", "B", "C", "D", "E", "F");

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
        Observable<Integer> o1=just(1, 2, 3, 4, 5)
          .delay(num -> timer(num*100, MILLISECONDS));

        Observable<Integer> o2=just(1, 2, 3, 4, 5)
                  .delay(num -> timer(num*200, MILLISECONDS));

        Observable<Integer> o3=just(1, 2, 3, 4, 5)
                  .delay(num -> timer(num*500, MILLISECONDS));

        Observable.merge(o1, o2, o3)
          .distinct()
          .subscribe(num -> System.out.printf("%d\n", num));

        Util.sleep(5000);
    }


    public void concatMap() {
        Observable<Long> obs=just(5L, 1L, 3L, 2L)
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