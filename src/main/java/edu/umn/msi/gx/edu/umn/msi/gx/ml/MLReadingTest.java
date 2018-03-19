package edu.umn.msi.gx.edu.umn.msi.gx.ml;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class MLReadingTest {

    //xmlns="http://psi.hupo.org/ms/mzml"

    public static void main(String[] args) throws IOException {

        String myTextFile = "/Users/mcgo0092/Documents/JavaCode/MZIdentXMLParser/data/msconvert_RAW.mzml";
        try (Stream<String> stream = Files.lines(Paths.get(myTextFile))) {

            boolean result = stream.filter(l -> l.contains("psi.hupo.org/ms/mzml")).findFirst().isPresent();

            System.out.println(result);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
