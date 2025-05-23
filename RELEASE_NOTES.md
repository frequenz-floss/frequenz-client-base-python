# Frequenz Client Base Library Release Notes

## Features

* Added support for HMAC signing of `UnaryUnary` client messages
* Added support for HMAC signing of `UnaryStream` client messages

## Upgrading

* Updated `protobuf` dependency range: changed from `>=4.21.6, <6` to `>=5.29.2, <7`
* The minimum dependency for `typing-extensions` is now `4.6.0` to be compatible with Python 3.12
* The minimum dependency for `grpcio` is now `1.59` to be compatible with Python 3.12

## Bug Fixes

* Fixed keys of signature to match what fuse-rs expects
* `GrpcStreamBroadcaster` will now correctly try to restart on unexpected errors.

    Before if an unexpected exception was raised by the stream method, the
    internal task would silently finish and never start again.
