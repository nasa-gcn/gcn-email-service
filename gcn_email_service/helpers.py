import logging
import threading

logger = logging.getLogger(__name__)


# Adapted from https://gist.github.com/Depado/7925679
def periodic_task(interval):
    def outer_wrap(function):
        def wrap(*args, **kwargs):
            stop = threading.Event()

            def inner_wrap():
                while not stop.is_set():
                    try:
                        function(*args, **kwargs)
                    except Exception:
                        logger.exception('Periodic task failed')
                    stop.wait(interval)

            t = threading.Thread(target=inner_wrap, daemon=True)
            t.start()
            return stop
        return wrap
    return outer_wrap
    