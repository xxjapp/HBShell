package tnode;

import java.io.IOException;

import exception.HBSException;

interface TNode {
    public abstract void handle()
    throws IOException, HBSException;
}
