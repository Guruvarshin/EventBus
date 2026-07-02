import pytest

from eventbus import Subscription


def test_subscription_is_immutable():
    sub = Subscription(id=1, event_type="user.created")
    with pytest.raises(AttributeError):
        sub.id = 2