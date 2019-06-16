package com.my.base.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by 89819 on 2018/3/12.
 */
public class TestDemo {
    /**
     * 业务功能
     *
     * @throws InterruptedException
     */
    public void handleBussiness() throws InterruptedException {
        System.out.println("client start working.....");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {

        List<ACL> acls = new ArrayList<ACL>();
        //添加第一个id，采用用户名密码形式
        //"digest" 模式

        Id id1 = new Id("digest", DigestAuthenticationProvider.generateDigest("admin:admin"));

        ACL acl1 = new ACL(ZooDefs.Perms.ALL, id1);
        acls.add(acl1);

        //添加第二个id，所有用户可读权限
        Id id2 = new Id("world", "anyone");
        ACL acl2 = new ACL(ZooDefs.Perms.READ, id2);
        acls.add(acl2);

        System.out.println(acls);

        ZooKeeper zk = new ZooKeeper("192.168.72.140:2181", 2000, null);
        zk.addAuthInfo("digest", "admin:admin".getBytes());
        //创建节点。  默认是创建永久。序列的不能单独存在
        //
        zk.create("/zk_test", "data".getBytes(), acls, CreateMode.PERSISTENT_SEQUENTIAL);
        //   zk.getChildren("/zk_test",true);
        //  zk.getData("/zk_test",true,null);
        //  zk.setData("/zk_test/test","badou".getBytes(),-1);

        zk.exists("/zk_test", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        });

    }
}