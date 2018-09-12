package com.wacai.stanlee.omega.util;

import java.util.*;
import java.util.stream.Stream;

/**
 * @author manshahua@wacai.com
 * @date 2017/12/22 下午2:16
 */
public class MapSort {
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValueAsc(Map<K, V> map) {
        Map<K, V> result = new LinkedHashMap<>();
        Stream<Map.Entry<K, V>> st = map.entrySet().stream();

        st.sorted(Comparator.comparing(e -> e.getValue())).forEach(e -> result.put(e.getKey(), e.getValue()));

        return result;
    }
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValueDesc(Map<K, V> map) {
        Map<K, V> result = new LinkedHashMap<>();
        Stream<Map.Entry<K, V>> st = map.entrySet().stream();

        st.sorted(Map.Entry.<K, V>comparingByValue().reversed()).forEachOrdered(e -> result.put(e.getKey(), e.getValue()));

        return result;
    }
    public static <K extends Comparable<? super K>, V > Map<K,V> sortByKeyAsc(Map<K,V> map)
    {
        Map<K, V> result = new LinkedHashMap<>();
        Stream<Map.Entry<K, V>> st = map.entrySet().stream();
        st.sorted(Comparator.comparing(e -> e.getKey())).forEach(e -> result.put(e.getKey(), e.getValue()));//升序

        return result;

    }
    public static <K extends Comparable<? super K>, V > Map<K,V> sortByKeyDesc(Map<K,V> map)
    {
        Map<K, V> result = new LinkedHashMap<>();
        Stream<Map.Entry<K, V>> st = map.entrySet().stream();
        st.sorted(Map.Entry.<K, V>comparingByKey().reversed()).forEachOrdered(e -> result.put(e.getKey(), e.getValue()));

        return result;

    }
    public static void main(String[] args)
    {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("a", "b");
        testMap.put("c", "c");
        testMap.put("b", "a");
        testMap.put("d", "d");
        LinkedHashMap<String, String> result = (LinkedHashMap<String, String>) sortByKeyAsc(testMap);
        for(Map.Entry<String, String> entry: result.entrySet())
        {
            String key = entry.getKey();
            String value = entry.getValue();
            System.out.println(key+": "+value);
        }
    }
}
