package edu.umn.msi.gx.java;

import java.util.ArrayList;
import java.util.List;

public class SpectrumIdentificationItem {

    String id;
    String spectrumIdentificationResultID;
    List<String> peptideEvidenceRef = new ArrayList<>();
    List<CVPair> params;
    List<CVPair> scores = new ArrayList<>();
    List<IonType> fragments = new ArrayList<>();

}
