
A collection of samples getting familiar with RxJava

Action items
------------

* Define timeout() on RpcDispatcher.callRemoteMethodsWithFuture(). Can a timeout be set on a CompletableFuture?
  http://www.esynergy-solutions.co.uk/blog/asynchronous-timeouts-completablefutures-java-8-and-java-9-raoul-gabriel-urma

* Reactive flow control in JGroups: look at whether reactive versions of UFC and MFC can be added

* Replace RspList with Observable<Rsp> instead? Each Observable would store its invocation ID
** We wouldn't need to wait until all Rsps have been received, but could possibly return as soon as a valid response
   was received (kind of like RspFilters)...
** This could be combined with a timeout on the observable

