package edu.umn.msi.gx.java;

public enum FDREnumeration {
    PEPTIDESHAKER("PeptideShaker PSM score"),
    MSGF("MS-GF:RawScore"),
    MORPHEUS("Morpheus:Morpheus score");

    private String fdrScore;

    FDREnumeration(String sName) {
        this.fdrScore = sName;
    }

    public String fdrScore() {
        return this.fdrScore;
    }

    @Override
    public String toString() {
        return this.fdrScore;
    }
}