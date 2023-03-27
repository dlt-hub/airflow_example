import dlt


@dlt.source
def dummy_source(prefix: str = None):
    @dlt.resource
    def dummy_data():
        for _ in range(100):
            yield f"{prefix}_dummy_data_{_}" if prefix else f"dummy_data_{_}"

    return dummy_data(),
