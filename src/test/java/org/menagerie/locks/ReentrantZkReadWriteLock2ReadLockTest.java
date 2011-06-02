package org.menagerie.locks;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.menagerie.BaseZkSessionManager;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;
import org.menagerie.util.TestingThreadFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static org.junit.Assert.assertTrue;
import static org.menagerie.TestUtils.newZooKeeper;

/**
 * @author Scott Fines
 *         Date: 5/30/11
 *         Time: 6:45 AM
 */
public class ReentrantZkReadWriteLock2ReadLockTest {

    private static final Logger logger = Logger.getLogger(ReentrantZkLock2Test.class);
    private static final String hostString = "localhost:2181";
    private static final String baseLockPath = "/test-readwrite-locks-2";
    private static final int timeout = 2000;
    private static final ExecutorService testService = Executors.newFixedThreadPool(2, new TestingThreadFactory());

    private static ZooKeeper zk;
    private static ZkSessionManager zkSessionManager;

    @Before
    public void setup() throws Exception {
        zk = newZooKeeper(hostString,timeout);

        //be sure that the lock-place is created
        ZkUtils.recursiveSafeDelete(zk, baseLockPath, -1);
        zk.create(baseLockPath,new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zkSessionManager = new BaseZkSessionManager(zk);
    }

    @After
    public void tearDown() throws Exception{
        try{
            List<String> children = zk.getChildren(baseLockPath,false);
            for(String child:children){
                ZkUtils.safeDelete(zk, baseLockPath +"/"+child,-1);
            }
            ZkUtils.safeDelete(zk, baseLockPath,-1);

        }catch(KeeperException ke){
            //suppress because who cares what went wrong after our tests did their thing?
        }finally{
            zk.close();
        }
    }

    @Test(timeout = 1000l)
    public void testTwoReadLocksCanAcquireSimultaneously() throws Exception{
        /*
        Tests that two different ReadLock instances can acquire the same lock simultaneously
        */
        final CountDownLatch latch = new CountDownLatch(1);
        ReadWriteLock rwLock = new ReentrantZkReadWriteLock2(baseLockPath, zkSessionManager);
        Lock readLock = rwLock.readLock();
        readLock.lock();
        try{
            Future<Void> future = testService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    ReadWriteLock rwLock2 = new ReentrantZkReadWriteLock2(baseLockPath,zkSessionManager);
                    Lock readLock2 = rwLock2.readLock();
                    readLock2.lock();
                    try{
                        latch.countDown();
                    }finally{
                        readLock2.unlock();
                    }
                    return null;
                }
            });

            //this will time out if the second lock can't acquire
            latch.await();

            //makre sure no exceptions are thrown
            future.get();
        }finally{
            readLock.unlock();
        }
    }

    @Test(timeout = 1000l)
    public void testOneReadLockDoesNotAllowDifferentWriteLockAccess() throws Exception{
        /*
        Tests that if a Read lock has the lock, then a different write lock instance can't acquire
        */
        final CountDownLatch latch = new CountDownLatch(1);
        ReadWriteLock rwLock = new ReentrantZkReadWriteLock2(baseLockPath,zkSessionManager);
        Lock readLock = rwLock.readLock();
        logger.debug("Acquiring read lock");
        readLock.lock();
        Future<Void> future;
        try{
            future = testService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {

                    ReadWriteLock secondLock = new ReentrantZkReadWriteLock2(baseLockPath,zkSessionManager);
                    Lock writeLock = secondLock.writeLock();
                    logger.debug("Attempting to acquire write lock");
                    writeLock.lock();
                    logger.debug("Write lock acquired");
                    try{
                        latch.countDown();
                    }finally{
                        logger.debug("Attempting to unlock write lock");
                        writeLock.unlock();
                        logger.debug("unlock of write lock successful");
                    }
                    return null;
                }
            });

            boolean notAcquired = !latch.await(250, TimeUnit.MILLISECONDS);
            assertTrue("The write lock was improperly acquired!",notAcquired);

        }finally{
            logger.debug("Attempting to unlock read lock");
            readLock.unlock();
            logger.debug("unlock of read lock successful");
        }

        //check that the lock gets acquired correctly
        final boolean acquired = latch.await(500, TimeUnit.SECONDS);
        assertTrue("Write lock was never acquired!",acquired);
        future.get();
    }

    @Test(timeout = 1000l)
    public void testReadLocksReentrant() throws Exception{
        /*
        Tests that, if you are on the same thread, you can access the read lock twice.

        This test will timeout if there is an error with the logic
        */
        ReadWriteLock rwLock = new ReentrantZkReadWriteLock2(baseLockPath,zkSessionManager);
        Lock readLock = rwLock.readLock();
        readLock.lock();
        readLock.lock();

        //now unlock twice to ensure that it's locks
        readLock.unlock();
        readLock.unlock();
    }

    @Test(timeout = 1000l)
    public void testTwothreadsHaveAccessViaSameReadLock() throws Exception{
        /*
        Tests that two threads, when using the same read lock, have the same amount of access

        This test will timeout if there is an error
        */
        final CountDownLatch latch = new CountDownLatch(1);
        ReadWriteLock lock = new ReentrantZkReadWriteLock2(baseLockPath,zkSessionManager);
        final Lock readLock = lock.readLock();
        readLock.lock();
        try{
            Future<Void> future = testService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    readLock.lock();
                    try{
                        latch.countDown();
                    }finally{
                        readLock.unlock();
                    }
                    return null;
                }
            });

            //wait for second thread to access lock
            latch.await();

            //check for errors
            future.get();
        }finally{
            readLock.unlock();
        }
    }
}
