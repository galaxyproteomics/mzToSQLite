/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umn.msi.gx.mztosqlite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import uk.ac.ebi.pride.tools.jmzreader.model.Spectrum;

/**
 *
 * @author James E Johnson jj@umn.edu
 * @version 
 */
public class PsmUtils {
    /*
    filters:
     "percent_tic": PercentTicFilterFactory,
     "percent_max_intensity": PercentMaxSpectrumIntensityFilterFactory,
     "quantile": QuantileFilterFactory,
     "mz_range_absolute": MzRangeFilterFactory,
     "mz_range_percent_bp": MzRangePercentBpFilterFactory,
     "intensity_range": IntensityRangeFilterFactory,
    MatchesIon:
      ions_matched
      peaks_matched
    def _setup_ion_matcher(self, settings, **options):
        mass_tolerance = DEFAULT_MASS_TOLERANCE
        # Could be defined for this column provider or whole evaluation
        # process.
        if 'mass_tolerance' in options:
            mass_tolerance = float(options['mass_tolerance'])
        elif 'mass_tolerance' in settings:
            mass_tolerance = float(settings['mass_tolerance'])
        self.ion_matcher = \
            lambda ion, peak: abs(peak[0] - ion.get_mz()) < mass_tolerance

    def _ions_matched(self, ions, peaks):
        ion_matcher = self.ion_matcher
        return [any([ion_matcher(ion, peak) for peak in peaks]) for ion in ions]

    def _peaks_matched(self, ions, peaks):
        # Probably not expected behavior
        ion_matcher = self.ion_matcher
        return [any([ion_matcher(ion, peak) for ion in ions]) for peak in peaks]
class UsesIonSeries(object):
    """
    >>> uses_ions = UsesIonSeries()
    >>> uses_ions._setup_ion_series({}, **{"ions": {"series": ["m1"]}})
    >>> from psme.peptide import Peptide
    >>> class TestPsm(): peptide = Peptide("AM")
    >>> psm = TestPsm()
    >>> ion = uses_ions._get_ions(psm)[0]
    >>> round(ion.get_mz(), 4)
    221.0954
    >>> uses_ions._setup_ion_series({"mass_type": "average"}, **{"ions": {"series": ["y1"]}})
    >>> ion = uses_ions._get_ions(psm)[0]
    >>> round(ion.get_mz(), 1)  # Should be 150.2206 but average doesn't really work.
    150.2
    """

    def _setup_ion_series(self, settings, **kwds):
        ion_options = kwds.get('ions', {}) or {}
        self.ion_options = ion_options

        if 'mass_type' in kwds:
            mass_type = kwds['mass_type']
        else:
            mass_type = settings.get('mass_type', DEFAULT_MASS_TYPE)
        self.calc_args = {'average': mass_type.lower().startswith('av')}

    def _get_ions(self, psm):
        return get_ions(psm.peptide, self.calc_args, **self.ion_options)

    AGGREGATE_ION_METHODS = ['count', 'count_longest_stretch', 'percent', 'count_missed', 'percent_missed', 'list_matches', 'list_misses']
    AGGREGATE_PEAKS_METHODS = ['count', 'percent', 'count_missed', 'percent_missed']

    
    Spectrum
      totalIonCurrent
      moz
      intensity
    Peptide
      seq
      modifications
      ions
    
    ions_matched
    
    
    class IonsMatched(ColumnProvider, AggregatesMatches, FiltersPeaks, MatchesIons):

    def __init__(self, settings, **kwds):
        super(IonsMatched, self).__init__(**kwds)
        self._setup_aggregate_by(aggregate_what='ions', **kwds)
        self._setup_peak_filters(**kwds)
        self._setup_ion_series(settings, **kwds)
        self._setup_ion_matcher(settings, **kwds)

    def calculate(self, spectra, psm):
        filtered_peaks = self._filtered_peaks(spectra)
        ions = self._get_ions(psm)
        matched = self._ions_matched(ions, filtered_peaks)
        return self._aggregate(matched, ions)


@register_column_provider(name="num_peaks")
class NumPeaks(ColumnProvider, FiltersPeaks):

    def __init__(self, **kwds):
        super(NumPeaks, self).__init__(**kwds)
        self._setup_peak_filters(**kwds)

    def calculate(self, spectra, psm):
        filtered_peaks = self._filtered_peaks(spectra)
        return len(filtered_peaks)


@register_column_provider(name="peaks_matched")
    stat: #|% (un) matched
    ion_series [abcxyx][123],M1,M2,internal
    losses: NH3,CO
    filters: 
        %TIC,
           %
        IntensityQuantile,
            q partitions
            k partition    
        Intensity%Max,
        mz AbsoluteRange, 
        mz %Range
class PeaksMatched(NumPeaks, MatchesIons, AggregatesMatches):

    def __init__(self, settings, **kwds):
        super(PeaksMatched, self).__init__(**kwds)
        self._setup_aggregate_by(aggregate_what='peaks', **kwds)
        self._setup_ion_series(settings, **kwds)
        self._setup_ion_matcher(settings, **kwds)

    def calculate(self, spectra, psm):
        filtered_peaks = self._filtered_peaks(spectra)
        ions = self._get_ions(psm)
        peaks_matched = self._peaks_matched(ions, filtered_peaks)
        return self._aggregate(peaks_matched)

    num_peaks
        filters:
            percent_tic
            quantile
            percent_max_intensity
            mz_range_absolute
            mz_range_percent_bp
    peaks_matched
        aggregate_by
            count
            count_missed
            percent
            percent_missed
        ions
            [abcxyx][123],M1,M2,internal
        losses: 
            H2O,NH3,CO           
        peak_filters
           percent_tic
            quantile
            percent_max_intensity
            mz_range_absolute
            mz_range_percent_bp
        tolerance_conditional
            0.5
    ions_matched
        aggregate_by
            count
            count_missed
            percent
            percent_missed
            list_matches
            list_misses
                [abcxyx][123],M1,M2,internal
        losses: 
            H2O,NH3,CO           
        peak_filters
            percent_tic
            quantile
            percent_max_intensity
            mz_range_absolute
            mz_range_percent_bp
        tolerance_conditional
            0.5
    
    
*/
    
    public static double totalIonCurrent(Spectrum s) {
        return 0;        
    }
    
    
    public void getFilteredPeaks(List<Double> mozArray,List<Double> intensityArray) {
        
       
    }
    public Map<Double, Double> getFilteredPeaks(Map<Double, Double> peakList) {
        
        return null;        
    }

    class IntensityThresholdFilterFactory {

        double min_inten;
        double max_inten;

        public IntensityThresholdFilterFactory(double min_inten, double max_inten) {
            this.min_inten = min_inten;
            this.max_inten = max_inten;
        }

        public Map<Double, Double> getFilteredPeaks(Map<Double, Double> peakList) {
            Map<Double, Double> filteredList = new HashMap<>();
            for (Double moz : peakList.keySet()) {
                Double inten = peakList.get(moz);
                if (min_inten <= inten && inten <= max_inten) {
                    filteredList.put(moz, inten);
                }
            }
            return filteredList;
        }
    }
}
