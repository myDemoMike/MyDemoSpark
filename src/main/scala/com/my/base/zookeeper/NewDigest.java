package com.my.base.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.util.ArrayList;
import java.util.List;

public class NewDigest {
    /**
     * 业务功能
     *
     * @throws InterruptedException
     */
    public void handleBussiness() throws InterruptedException {
        System.out.println("client start working.....");
        Thread.sleep(Long.MAX_VALUE);
    }
    //某个数据它既绑定了exists节点又geiData这个监控，那当这个数据被删除的时候只能触发被删除的通知。

    public static void main(String[] args) throws Exception {

        List<ACL> acls = new ArrayList<ACL>();

        //(scheme:expression,perms)
        // expression  权限  perms  用户
        //添加第一个id，采用用户名密码形式
        Id id1 = new Id("digest", DigestAuthenticationProvider.generateDigest("admin:admin"));
        ACL acl1 = new ACL(ZooDefs.Perms.ALL, id1);
        acls.add(acl1);

        //添加第二个id，所有用户可读权限
        Id id2 = new Id("world", "anyone");
        ACL acl2 = new ACL(ZooDefs.Perms.READ, id2);
        acls.add(acl2);

        System.out.println(acls);

        // zk用admin认证，创建/test ZNode。
//        192.168.91.128:2181,192.168.91.129:2181,192.168.91.130:2181
        ZooKeeper zk = new ZooKeeper("192.168.91.128:2181,192.168.91.129:2181,192.168.91.130:2181", 2000, null);
        zk.addAuthInfo("digest", "admin:admin".getBytes());
//        zk.create("/zk_test/badou", "data".getBytes(), acls, CreateMode.PERSISTENT_SEQUENTIAL);
//        zk.setData("/zk_test/test","badou".getBytes(),-1);

//        zk.getChildren("/zk_test",true);
//        zk.getData("/zk_test",true,null);
        zk.exists("/zk_test", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        });


//        在进行数据监控的时候有效
//        zk.setData("/zk_test","a123".getBytes(),-1);
//        byte[] data = zk.getData("/zk_test",false, null);
//        System.out.println(new String(data));
//
//
//
//        List<String> children = zk.getChildren("/zk_test", true);
//        for (String child : children) {
//            System.out.println(child);
//        }


//        临时节点注册测试，让这个保持运行，看临时节点的存活情况
//        System.out.println("client start working.....");
        Thread.sleep(Long.MAX_VALUE);
    }
}
