package org.menagerie.locks;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 *         Date: 5/27/11
 *         Time: 2:46 PM
 */
final class LockHolder{
    private final AtomicReference<Thread> holdingThread = new AtomicReference<Thread>();
    private volatile String lockNode;
    private final AtomicInteger holdCount = new AtomicInteger(0);

    public void setHoldingThread(String lockNode){
        holdingThread.set(Thread.currentThread());
        holdCount.set(1);
        this.lockNode = lockNode;
    }

    boolean increment(){
        if(Thread.currentThread().equals(holdingThread.get())){
            holdCount.incrementAndGet();
            return true;
        }else{
            return false;
        }
    }

    int decrement(){
        if(Thread.currentThread().equals(holdingThread.get())){
            int count = holdCount.decrementAndGet();
//            if(count<=0){
//                holdingThread.set(null);
//            }
            return count;
        }else{
            return holdCount.get();
        }
    }

    String getLockNode(){
        if(Thread.currentThread().equals(holdingThread.get()))
            return lockNode;
        else
            return null;
    }

    void clear(){
        holdingThread.set(null);
        holdCount.set(0);
    }
}
