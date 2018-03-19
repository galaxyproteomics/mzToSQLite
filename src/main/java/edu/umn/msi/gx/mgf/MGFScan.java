package edu.umn.msi.gx.mgf;

import edu.umn.msi.galaxyp.java.CVPair;
import edu.umn.msi.galaxyp.java.FloatPair;
import edu.umn.msi.gx.java.CVPair;
import edu.umn.msi.gx.java.FloatPair;

import java.util.ArrayList;
import java.util.List;

public class MGFScan {
    /**
     * Snip
     * BEGIN IONS
     PEPMASS=550.179504
     CHARGE=3+
     SCANS=900
     TITLE=Mo_Tai_iTRAQ_f1.00900.00900.3
     110.070961 228.732956
     113.070747 400.866913
     114.110413 1430.571899
     115.107475 850.522217
     116.465904 100.048393
     122.499107 80.226799
     122.513062 84.079254
     125.084969 91.535202
     129.101776 94.549103
     134.172043 81.021111
     136.075470 177.632813
     140.068756 99.546608
     145.113297 111.233635
     149.044739 118.262764
     150.915970 89.228943
     */
    public List<CVPair> metaData = new ArrayList<>();
    public List<FloatPair> mzIntensities = new ArrayList<>();
    public int scanIndex; //1 based
    public String sourceFile;

    public String getSourceFile(){
        return this.sourceFile;
    }

    public String getSpectrumID() {
        return "index=" + this.scanIndex;
    }
    public void setSourceFile(String fName) { this.sourceFile = fName;}

    public String getSpectrumTitle() {
        String rVal = null;
        for (CVPair p : this.metaData) {
            if (p.name.equals("TITLE")) {
                rVal = p.value;
            }
        }
       return rVal;
    }

    public String getMZValueList() {
        if (this.mzIntensities.size() == 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder();

        sb.append("[");
        for (FloatPair p : this.mzIntensities) {
            sb.append(String.valueOf(p.a));
            sb.append(",");
        }
        sb.deleteCharAt(sb.lastIndexOf(","));
        sb.append("]");

        return sb.toString();
    }

    public String getIntensityValueList() {

        if (this.mzIntensities.size() == 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder();

        sb.append("[");
        for (FloatPair p : this.mzIntensities) {
            sb.append(String.valueOf(p.b));
            sb.append(",");
        }
        sb.deleteCharAt(sb.lastIndexOf(","));
        sb.append("]");

        return sb.toString();
    }

    public float getTIC() {
        float tic = 0.0f;
        for (FloatPair s : this.mzIntensities) {
            tic += s.a;
        }
        return tic;
    }
}
