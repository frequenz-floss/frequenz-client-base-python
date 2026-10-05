# Frequenz Client Base Library Release Notes

## Summary

This release adds support for protobuf 7 and fixes HTTP/2 keep-alive, which was never actually enabled.

## Upgrading

- The protobuf requirement is now `>=5.29.2,<8`, allowing protobuf 7 while retaining support for protobuf 5 and 6.

## Bug Fixes

- HTTP/2 keep-alive is now actually enabled. `grpc.keepalive_time_ms` and `grpc.keepalive_timeout_ms` were computed with `timedelta.total_seconds() * 1000` and therefore passed as `float`. gRPC silently ignores channel arguments that are neither `int` nor `str`, so no keep-alive pings were ever sent and a client could only detect a dead stream through its own deadline or a teardown sent by the peer.

  Note that keep-alive pings are answered by the nearest HTTP/2 peer. Where a proxy terminates HTTP/2, successful pings prove that hop is alive, not the backend, so a stream deadline is still needed to detect an orphaned upstream stream.
