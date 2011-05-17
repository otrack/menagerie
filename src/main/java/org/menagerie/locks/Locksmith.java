package org.menagerie.locks;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.menagerie.Beta;
import org.menagerie.ZkCommandExecutor;
import org.menagerie.ZkSessionManager;

import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * TODO: Add documentation describing that this is the appropriate manner to create a lock
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


}
