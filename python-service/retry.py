import types


def retry(
    fn: types.FunctionType,
    fallback: types.FunctionType = lambda: None,
    max_retries: int = 10,
):
    assert fn is not None, "you must provide a valid function"
    assert max_retries > -1, "you must retry 0 or more times"
    retry_count = 0
    while retry_count < max_retries:
        try:
            retry_count += 1
            return fn()
        except Exception:
            pass
    return fallback()


def retryable(max_retries=5, fallback: types.FunctionType = None):
    def decorator_repeat(func):
        def wrapper_repeat(*args, **kwargs):
            retry_count = 0
            while retry_count <= max_retries:
                try:
                    print("retry # %s" % retry_count)
                    retry_count += 1
                    return func(*args, **kwargs)
                except Exception:
                    pass
            if fallback is not None:
                return fallback(*args, **kwargs)
            return None

        return wrapper_repeat

    return decorator_repeat


if __name__ == "__main__":

    @retryable(max_retries=5)
    def bad(name: str):
        return "hello %s" % name

    @retryable(max_retries=5)
    def badder(name: str):
        if name.lower() == "josh".lower():
            raise Exception("danger!")
        return bad(name)

    @retryable(fallback=lambda n: "oops")
    def baddest(name: str):
        return badder(name)

    assert bad("jane") == "hello jane"
    assert badder("josh") is None
    assert baddest("josh") == "oops"
