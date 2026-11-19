from seasentinel.detectors.gap import GapAlert


def alert(jump):
    return GapAlert(672000001, 0, 7200, 34, 10, 34, 10, jump, 0.0)


def test_severity_levels():
    assert alert(0.0).severity == "faible"
    assert alert(0.99).severity == "faible"
    assert alert(1.0).severity == "moyenne"
    assert alert(19.9).severity == "moyenne"
    assert alert(20.0).severity == "elevee"
    assert alert(232.0).severity == "elevee"
