package tnode;

import java.io.IOException;

import exception.HBSException;

public interface TNode {
    public abstract void handle()
    throws IOException, HBSException;
}
