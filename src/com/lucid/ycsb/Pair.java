package com.lucid.ycsb;

public class Pair<U, V> {

    private U obj1;

    private V obj2;

    public Pair(U u, V v) {
        obj1 = u;
        obj2 = v;
    }

    public U getObj1() {
        return obj1;
    }

    public void setObj1(U obj1) {
        this.obj1 = obj1;
    }

    public V getObj2() {
        return obj2;
    }

    public void setObj2(V obj2) {
        this.obj2 = obj2;
    }
}
