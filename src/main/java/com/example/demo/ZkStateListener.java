package com.example.demo;

import org.apache.zookeeper.Watcher;

public interface ZkStateListener {

    public void stateChanged(Watcher.Event.KeeperState state) throws Exception;

    public void handleNewSession() throws Exception;

    public void handleSessionEstablishmentError(final Throwable error) throws Exception;


}
