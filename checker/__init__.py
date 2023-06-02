from check_update import checkForUpdate
import schedule
import sentry_sdk
import time

if __name__ == "__main__":
    sentry_sdk.init(
        dsn="https://215c17a6719441f591985436380839cf@o4504585220980736.ingest.sentry.io/4504781485309952",
        traces_sample_rate=1.0
    )
    x = checkForUpdate()
    schedule.every(5).minutes.do(x.run)
    while True:
        schedule.run_pending()
        time.sleep(60)