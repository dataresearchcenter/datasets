import ijson


def handle(ctx):
    """Extract records from the lobbyregister JSON API response using streaming."""
    with ctx.open() as fh:
        for record in ijson.items(fh, "results.item"):
            yield record
