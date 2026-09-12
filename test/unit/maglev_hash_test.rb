# frozen_string_literal: true

require "test_helper"

class MaglevHashTest < ActiveSupport::TestCase
  # The two fixture tests mirror Solid Cache's own, so our copy provably
  # distributes keys exactly like the original
  test "two nodes" do
    maglev_hash = SolidQueue::MaglevHash.new([ :node1, :node2 ])
    results = nodes_for_1_to_30(maglev_hash)

    assert_equal [ 3, 5, 8, 11, 12, 14, 18, 19, 20, 22, 27, 28, 29 ], results[:node1]
    assert_equal [ 1, 2, 4, 6, 7, 9, 10, 13, 15, 16, 17, 21, 23, 24, 25, 26, 30 ], results[:node2]
  end

  test "three nodes" do
    maglev_hash = SolidQueue::MaglevHash.new([ :node1, :node2, :node3 ])
    results = nodes_for_1_to_30(maglev_hash)

    assert_equal [ 5, 18, 20, 22, 27, 28, 29 ], results[:node1]
    assert_equal [ 1, 2, 4, 7, 9, 10, 13, 15, 21, 23, 26, 30 ], results[:node2]
    assert_equal [ 3, 6, 8, 11, 12, 14, 16, 17, 19, 24, 25 ], results[:node3]
  end

  test "adding a node moves only a fraction of the keys" do
    keys = (1..300).map { |i| "key-#{i}" }
    two = SolidQueue::MaglevHash.new(%i[ node1 node2 ])
    three = SolidQueue::MaglevHash.new(%i[ node1 node2 node3 ])

    moved = keys.count { |key| two.node(key) != three.node(key) }
    adopted = keys.count { |key| three.node(key) == :node3 }

    assert_operator adopted, :>, 0
    assert_operator moved, :<, keys.size / 2, "Adding a node should move a minority of keys, moved #{moved} of #{keys.size}"
  end

  test "node order doesn't matter" do
    keys = (1..50).map { |i| "key-#{i}" }
    sorted = SolidQueue::MaglevHash.new(%i[ node1 node2 node3 ])
    shuffled = SolidQueue::MaglevHash.new(%i[ node3 node1 node2 ])

    keys.each { |key| assert_equal sorted.node(key), shuffled.node(key) }
  end

  test "node count limits" do
    assert_raises(ArgumentError) { SolidQueue::MaglevHash.new([]) }
    assert_raises(ArgumentError) { SolidQueue::MaglevHash.new(2054.times.map(&:to_s)) }
  end

  private
    def nodes_for_1_to_30(maglev_hash)
      results = Hash.new { |hash, key| hash[key] = [] }
      (1..30).each { |key| results[maglev_hash.node(key)] << key }
      results
    end
end
