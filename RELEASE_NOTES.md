# Frequenz Client Base Library Release Notes

## Upgrading

* There is very minor breaking change in this release, `GrpcStreamBroadcaster` now requires a `grpc.aio.UnaryStreamCall` instead of a `AsyncIterable` for the `stream_method` argument. In practice all users should be passing a `grpc.aio.UnaryStreamCall` already, so this should not affect anyone unless they are doing something very strange.

## Bug Fixes

* `GrpcStreamBroadcaster`: Fix potential long delays on retries, and giving up early if the number of retries is limited.

   The retry strategy was not reset after a successful start of the stream, so the back-off delays would accumulate over multiple retries and eventually give up if the number of retries were limited, even if there was a successful start of the stream in between. Now we properly reset the retry strategy after a successful start of the stream (successfully receiving the first item from the stream).

* `GrpcStreamBroadcaster`: Fix `StreamStarted` event firing too soon.

   The `StreamStarted` event was being fired as soon as the streamming method was called, but that doesn't mean that a streamming connection was established with the server at all, which can give a false impression that the stream is active and working. Now we wait until we receive the initial metadata from the server before firing the `StreamStarted` event. That should give users a better indication that the stream is actually active and working without having to wait for the first item to be received, which can take a long time for some low-frequency streams.
