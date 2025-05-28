# Frequenz Client Base Library Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

* The streaming client now also sends state change events out. Usage example:
```python
    recv = streamer.new_receiver()

    for msg in recv:
        match msg:
            case StreamStartedEvent():
                print("Stream started")
            case StreamStoppedEvent() as event:
                print(f"Stream stopped, reason {event.exception}, retry in {event.retry_time}")
            case int() as output:
                print(f"Received message: {output}")
```

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
