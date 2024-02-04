package simpledb.transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

public class LockManager {
    private Map<PageId, List<Lock>> map;

    public LockManager() {
        this.map = new ConcurrentHashMap<>();
    }

    public synchronized Boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) {
        List<Lock> locks = map.get(pid);
        Lock newLock = new Lock(perm, tid);
        if (locks == null) {
            // 该页面未加过锁
            locks = new ArrayList<>();
            locks.add(newLock);
            map.put(pid, locks);
            return true;
        }
        if (locks.size() == 1) {
            // 一个锁，可能是共享锁，也可能是独占锁
            Lock oldLock = locks.get(0);
            if (oldLock.getTransactionId()
                .equals(tid)) {
                // 同个事务
                if (oldLock.getPermissions().equals(Permissions.READ_ONLY) && perm.equals(Permissions.READ_WRITE)) {
                    // 之前是共享锁，现在要加独占锁 => 锁升级
                    oldLock.setPermissions(Permissions.READ_WRITE);
                }
                // 同个事务 写 写，读 读都可以，且不需要重复加锁。写 读：因为读写锁是兼容只读锁的，所以也不需要重复加锁
                return true;
            } else {
                if (oldLock.getPermissions().equals(Permissions.READ_ONLY) && perm.equals(Permissions.READ_ONLY)) {
                    locks.add(newLock);
                    return true;
                } else {
                    // 不同事务的话，读 写，写 读，写 写都不行
                    return false;
                }
            }
        }

        // 能存在多个锁，所以是不同事务的读锁
        if (newLock.getPermissions().equals(Permissions.READ_WRITE)) {
            return false;
        }

        for (Lock lock : locks) {
            // 同个事务，不需要重复加只读锁
            if (lock.getTransactionId()
                .equals(tid)) {
                return true;
            }
        }

        locks.add(newLock);
        return true;
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        List<Lock> locks = map.get(pid);
        for (Lock lock : locks) {
            if (lock.getTransactionId()
                .equals(tid)) {
                return true;
            }
        }
        return false;
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        List<Lock> locks = map.get(pid);
        for (Lock lock : locks) {
            if (lock.getTransactionId()
                .equals(tid)) {
                locks.remove(lock);
                if (locks.size() == 0) {
                    map.remove(pid);
                }
                return;
            }
        }
    }

    public void releaseAllLocks(TransactionId tid) {
        for (PageId pid : map.keySet()) {
            releaseLock(tid, pid);
        }
    }
}
