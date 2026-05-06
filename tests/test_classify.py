from app.classify import guess_movie_or_tv


def test_guess_single_long_is_movie() -> None:
    assert guess_movie_or_tv([{"duration_seconds": 7000}]) == "movie"


def test_guess_many_short_is_tv() -> None:
    titles = [{"duration_seconds": 22 * 60} for _ in range(5)]
    assert guess_movie_or_tv(titles) == "tv"
