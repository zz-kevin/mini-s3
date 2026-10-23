from collections import Counter

from mini_s3.ring import HashRing


def test_preference_list_is_distinct_and_complete():
    ring = HashRing("abcde")
    for i in range(200):
        order = ring.preference_list(f"bucket/key-{i}")
        assert sorted(order) == list("abcde")


def test_same_key_same_nodes():
    assert HashRing("abcd").preference_list("x/y") == HashRing("abcd").preference_list("x/y")


def test_keys_spread_evenly():
    ring = HashRing("abcd")
    owners = Counter(ring.preference_list(f"b/k{i}")[0] for i in range(20000))
    # With 64 virtual nodes each, every node should own roughly 25% of keys.
    assert all(0.15 < n / 20000 < 0.35 for n in owners.values()), owners


def test_adding_a_node_moves_few_keys():
    keys = [f"b/k{i}" for i in range(5000)]
    before = HashRing("abcd")
    after = HashRing("abcde")
    moved = sum(before.preference_list(k)[0] != after.preference_list(k)[0] for k in keys)
    # Ideal is 1/5 of keys moving to the new node; naive hash % n would move ~80%.
    assert moved / len(keys) < 0.3


def test_remove_node():
    ring = HashRing("abc")
    ring.remove("b")
    assert ring.nodes == ["a", "c"]
    assert "b" not in ring.preference_list("x/y")
