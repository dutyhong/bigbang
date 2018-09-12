package com.wacai.stanlee.omega.bang;

/**
 * @author manshahua@wacai.com
 * @date 2018/1/24 下午5:58
 */
public class IntHeap {
    public int all;
    private Node begin;
    private Node end;
    private Node point;
    private int length;
    public int maxlen;
    boolean hasNext;
    private int n;
    private int m;

    public IntHeap(int n, int m) {
        all = 0;
        hasNext = true;
        maxlen = 0;
        length = 0;
        point = new Node();
        begin = end = point;
        this.n = n;
        this.m = m;
    }

    public void init() {
        hasNext = true;
        length = 0;
        point = new Node();
        begin = end = point;
    }

    public int Pop() {
        if (isEmpty() || end.before == null) return 99999;
        point = end.before;
        int temp = point.key;
        if (point == begin) {
            init();
        } else {
            point = point.before;
            point.next = end;
            end.before = point;
            length--;
        }
        return temp;
    }

    public int Pushit(int num) {
        int next = num;
        if (isFull()) return -1;
        Node temp = new Node();
        point = end;
        point.key = next;
        point.next = temp;
        temp.before = point;
        point = temp;
        end = point;
        length++;
        return num;
    }

    public int makeit(int num) {
        if (num > (n - 1)) return -1;
        if (num < (n - 1)) {
            if (isFull()) {
                makeit(Pop());
            } else {
                Pushit(num + 1);
                if (isLastOne()) hasNext = false;
            }
        } else {
            Pop();
            makeit(Pop());
            makeit(last());
        }
        return 0;
    }

    public boolean isLastOne() {
        Node t = begin;
        int p = n - m;
        boolean b = true;
        while (t != end) {
            b = b && (t.key == p);
            t = t.next;
            p++;
        }
        return b;
    }

    public boolean isEmpty() {
        return (length == 0);
    }

    public boolean isFull() {
        return (length == maxlen);
    }

    public String toString() {
        Node temp = begin;
        String str = "";
        while (temp != end) {
            str += temp.key + ";";
            temp = temp.next;
        }
        return str;
    }

    public int last() {
        Node t = end;
        if (!isEmpty()) return t.before.key;
        else return -1;
    }
}
