package com.hmdp.utils;

public interface ILock {

    boolean tryLocks(long timeoutSec);

    void unlock();
}
