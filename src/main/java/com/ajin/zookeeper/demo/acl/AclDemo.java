package com.ajin.zookeeper.demo.acl;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;

/**
 * {@link ACL} Demo
 *
 * @author ajin
 */

public class AclDemo {

    private static final String CONNECT_STRING  = "127.0.0.1:21810";
    private static final int COMMON_SESSION_TIME_OUT = 5000;

    private static final String ACL_PATH       = "/zk-acl-path";
    private static final String ACL_CHILD_PATH = "/zk-acl-path/child";

    public static void main(String[] args) throws Exception {
        // testGetAclPathWithoutAuthAfterCreate(ACL_PATH);
        // testGetAclPathWithAuthAfterCreate(ACL_PATH);
        testGetAclPathWith(ACL_PATH);
    }
    private static void testGetAclPathWith(String aclPath) throws InterruptedException, IOException, KeeperException {
        createAuthPath(aclPath);
        // [B@44e81672
        System.out.println(getAclPathWithAuth(aclPath));
        // Exception in thread "main" org.apache.zookeeper.KeeperException$NoAuthException: KeeperErrorCode = NoAuth for /zk-acl-path
        System.out.println(getAclPathWithOutAuth(aclPath));

    }

    private static void testGetAclPathWithAuthAfterCreate(String aclPath)
        throws InterruptedException, IOException, KeeperException {
        createAuthPath(aclPath);
        // [B@44e81672
        System.out.println(getAclPathWithAuth(aclPath));
    }

    /**
     * 测试  在创建某个带有ACL权限的数据节点后 使用不带权限的客户端去访问数据节点
     *
     * <code>
     * Exception in thread "main" org.apache.zookeeper.KeeperException$NoAuthException: KeeperErrorCode = NoAuth for /zk-acl-path
     * at org.apache.zookeeper.KeeperException.create(KeeperException.java:116)
     * at org.apache.zookeeper.KeeperException.create(KeeperException.java:54)
     * at org.apache.zookeeper.ZooKeeper.getData(ZooKeeper.java:1215)
     * at org.apache.zookeeper.ZooKeeper.getData(ZooKeeper.java:1244)
     * at com.ajin.zookeeper.demo.acl.AclDemo.getAclPathWithOutAuth(AclDemo.java:38)
     * at com.ajin.zookeeper.demo.acl.AclDemo.testGetAclPathWithoutAuthAfterCreate(AclDemo.java:32)
     * at com.ajin.zookeeper.demo.acl.AclDemo.main(AclDemo.java:23)
     * </code>
     **/
    private static void testGetAclPathWithoutAuthAfterCreate(String aclPath)
        throws InterruptedException, IOException, KeeperException {
        createAuthPath(aclPath);
        getAclPathWithOutAuth(aclPath);
    }

    private static byte[] getAclPathWithAuth(String aclPath)
        throws InterruptedException, IOException, KeeperException {
        ZooKeeper zooKeeper = getZooKeeper();
        zooKeeper.addAuthInfo("digest", "access:true".getBytes());
        return zooKeeper.getData(aclPath, false, null);
    }

    private static byte[] getAclPathWithOutAuth(String aclPath)
        throws InterruptedException, IOException, KeeperException {
        ZooKeeper zooKeeper = getZooKeeper();

        return zooKeeper.getData(aclPath, false, null);
    }

    private static ZooKeeper getZooKeeper() throws IOException {
        return new ZooKeeper(CONNECT_STRING, COMMON_SESSION_TIME_OUT, null);
    }

    private static void createAuthPath(String path) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zooKeeper = getZooKeeper();
        zooKeeper.addAuthInfo("digest", "access:true".getBytes());
        zooKeeper.create(path, "init".getBytes(), Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);
    }

}
