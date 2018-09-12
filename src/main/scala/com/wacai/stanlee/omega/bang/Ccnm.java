package com.wacai.stanlee.omega.bang;


import java.util.ArrayList;
import java.util.List;

/**
 * @author manshahua@wacai.com
 * @date 2018/1/22 上午11:09
 */
public class Ccnm{
    private int n = 2;
    private int m = 1;
    public Ccnm(int setn,int setm){
        n = setn;
        m = setm;
    }

    public List<String> tt(){
        IntHeap t = new IntHeap(n, m);
        t.maxlen = m;
        List<String> result = new ArrayList<String>();
        t.Pushit(0);
        while(t.hasNext){
            do{
                t.makeit(t.last());
            }while(!t.isFull());
//            System.out.println(t.toString());
            result.add(t.toString());
            t.all++;
        }
//        System.out.println("The all="+t.all);
        return result;
    }
    public static void main(String[] args){
        Ccnm tt = new Ccnm(500,2);
        double start = System.currentTimeMillis();
        List<String> result = tt.tt();
        double end = System.currentTimeMillis();
        System.out.println("耗时"+(end-start)/(1000));
//        for(int i=0; i<result.size(); i++)
//        {
//            String tmp = result.get(i);
//            String[] tmp1 = tmp.split(";");
//            int num = tmp1.length;
//        }
        System.out.println("ddd");
    }
}
