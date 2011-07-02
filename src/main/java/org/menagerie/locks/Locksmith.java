package org.menagerie.locks;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.menagerie.Beta;
import org.menagerie.ZkCommandExecutor;
import org.menagerie.ZkSessionManager;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Static factories for creating and using ZooKeeper-based Locks.
 *
 * <p>These methods wrap public constructors in specific classes, but
 * using those constructors is <em>not</em> recommended.
 */
@Beta
public class Locksmith {

    public static final Lock reentrantLock(ZkSessionManager sessionManager, String lockPath) {
        return new ReentrantZkLock2(lockPath, sessionManager, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    public static final Lock reentrantLock( String lockPath, List<ACL> privileges, ZkCommandExecutor exec){
        return new ReentrantZkLock2(lockPath, exec, privileges);
    }

    public static final Lock reentrantLock(ZkSessionManager sessionManager, String lockPath, List<ACL> privileges){
        return new ReentrantZkLock2(lockPath, sessionManager, privileges);
    }


    public static final ReadWriteLock readWriteLock(ZkSessionManager sessionManager,String lockPath,List<ACL> privileges){
        return new ReentrantZkReadWriteLock2(lockPath,sessionManager,privileges);
    }

    public static final ReadWriteLock readWriteLock(ZkSessionManager sessionManager,String lockPath){
        return new ReentrantZkReadWriteLock2(lockPath,sessionManager);
    }

    public static final ReadWriteLock readWriteLock(ZkCommandExecutor commandExecutor,String lockPath,List<ACL> privileges){
        return new ReentrantZkReadWriteLock2(lockPath,commandExecutor,privileges);
    }




}
