package edu.umn.msi.gx.java;

import java.util.*;
import java.util.stream.Stream;

public class IonType {

    public String name;
    public int charge;
    public Integer[] ionIndex;
    public int numConsecutive = 0;
    public double pctMatchedIons = 0.0;
    public Map<String, String> fragmentArray = new HashMap<>();

    public void setCharge(String c){
        this.charge = Integer.valueOf(c);
    }

    public void setName(String n) {
        this.name = n.replace("frag: ", "");
    }

    public void calculateIonCoverage(int maxIonCount) {
        switch (this.name) {
            case "b ion":
            case "y ion":
            case "z ion":
                this.pctMatchedIons  = ((double)this.ionIndex.length / maxIonCount) * 100.0;
                break;
        }
    }

    public void calcIons(String attrIndex) {

        String[] strI = attrIndex.split(" ");
        Stream<Integer> ionStream = Arrays.stream(strI)
                .map(Integer::valueOf);

        this.ionIndex = ionStream.toArray(Integer[]::new);
        HashSet<Integer> ionSet = new HashSet<>();
        int ans = 0;

        Collections.addAll(ionSet, this.ionIndex);
        for (Integer anIonIndex : this.ionIndex) {
            // if current element is the starting
            // element of a sequence
            if (!ionSet.contains(anIonIndex - 1)) {
                // Then check for next elements in the
                // sequence
                int j = anIonIndex;
                while (ionSet.contains(j))
                    j++;

                // update  optimal length if this length
                // is more
                if (ans < j - anIonIndex)
                    ans = j - anIonIndex;
            }
        }
        numConsecutive = ans;

    }
}
