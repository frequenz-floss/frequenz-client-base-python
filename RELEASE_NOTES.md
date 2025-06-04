# Frequenz Client Base Library Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

* Updated interface and behavior for HMAC

    This introduces a new positional argument to `parse_grpc_uri`.
    If calling this function manually and passing `ChannelOptions`, it is recommended
    to switch to passing `ChannelOptions` via keyword argument.

* All parameters of the Streamers `new_receiver` method are now keyword-only arguments. This means that you must specify them by name when calling the method, e.g.:

    ```python
    recv = streamer.new_receiver(max_size=50, warn_on_overflow=True)
    ```

## New Features

* The streaming client, when using `new_receiver(include_events=True)`, will now return a receiver that yields stream notification events, such as `StreamStarted`, `StreamRetrying`, and `StreamFatalError`. This allows you to monitor the state of the stream:

    ```python
    recv = streamer.new_receiver(include_events=True)

    for msg in recv:
        match msg:
            case StreamStarted():
                print("Stream started")
            case StreamRetrying(delay, error):
                print(f"Stream stopped and will retry in {delay}: {error or 'closed'}")
            case StreamFatalError(error):
                print(f"Stream will stop because of a fatal error: {error}")
            case int() as output:
                print(f"Received message: {output}")
    ```

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
