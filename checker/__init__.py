from check_update import checkForUpdate
import sentry_sdk

if __name__ == "__main__":
    sentry_sdk.init(
        dsn="https://aadfc763299c4541b15f33e414c395d4@o4504585220980736.ingest.sentry.io/4504649935945728",
        traces_sample_rate=1.0
    )
    x = checkForUpdate()
    x.run()