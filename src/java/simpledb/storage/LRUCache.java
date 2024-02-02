package simpledb.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LRUCache<K, V> {
    class DLinkedNode {
        K key;
        V val;
        DLinkedNode prev;
        DLinkedNode next;

        public DLinkedNode() {
        }

        public DLinkedNode(K key, V val) {
            this.key = key;
            this.val = val;
        }
    }

    private Map<K, DLinkedNode> cache = new ConcurrentHashMap<>();
    private int size;
    private int capacity;
    private DLinkedNode head, tail;

    public LRUCache(int capacity) {
        this.size = 0;
        this.capacity = capacity;

        head = new DLinkedNode();
        tail = new DLinkedNode();

        head.next = tail;
        tail.prev = head;
    }

    public V get(K key) {
        DLinkedNode node = cache.get(key);
        if (node == null) {
            return null;
        }
        moveToHead(node);
        return node.val;
    }

    public boolean containKey(K key) {
        return cache.containsKey(key);
    }

    public void put(K key, V val) {
        DLinkedNode node = cache.get(key);
        if (node == null) {
            node = new DLinkedNode(key, val);
            addToHead(node);
            cache.put(key, node);
            ++size;
        } else {
            node.val = val;
            moveToHead(node);
        }
    }

    private void addToHead(DLinkedNode node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    public void remove(K key) {
        DLinkedNode node = cache.get(key);
        if (node == null) {
            return;
        }
        removeNode(node);
        cache.remove(key);
        --size;
    }

    private void removeNode(DLinkedNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void moveToHead(DLinkedNode node) {
        removeNode(node);
        addToHead(node);
    }

    public int getSize() {
        return size;
    }

    public DLinkedNode getHead() {
        return head;
    }

    public DLinkedNode getTail() {
        return tail;
    }

    public int getCapacity() {
        return capacity;
    }

    public Map<K, DLinkedNode> getCache() {
        return cache;
    }

    public void discord() {
        DLinkedNode tail = removeTail();
        cache.remove(tail.key);
        --size;
    }

    public DLinkedNode removeTail() {
        DLinkedNode tail = this.tail.prev;
        removeNode(tail);
        return tail;
    }
}
