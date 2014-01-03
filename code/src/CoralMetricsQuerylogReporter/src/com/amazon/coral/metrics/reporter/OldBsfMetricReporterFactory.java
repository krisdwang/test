package com.amazon.coral.metrics.reporter;

import com.amazon.coral.metrics.Group;
import javax.measure.unit.Unit;
import java.util.HashMap;
import java.util.Random;

/**
 * The {@code OldBsfMetricReporterFactory} is a reporter factory that is
 * placed before the aggregating {@code ReporterFactory}. We hope to
 * delete this code one day soon.
 *
 * (From discussion with Eric Crahen, Monty Vanderbilt, Dave Killian)
 * The Remote metric in Coral exhibts the
 * same trends as Remote1 and is directly relatable to Latency, making it
 * a cheaper, more effective metric for monitoring.
 *
 * This reporter allows us to measure 1 data point, randomly, before
 * aggregation. The only reason this reporter needs to exist is to provide
 * the Remote1 metric.
 *
 * Remote1 exists only because the BSF/JST frameworks were not able to
 * measure Remote as an average accurately. As a work around, Remote1 was
 * created so people could monitor services. Coral is able to measure
 * Remote correctly, making Remote1 unneccessary.
 *
 * As of 02/21/09, PMET does not expose the Remote metric to users. An
 * upcoming project is planned that allows users to select metrics, and
 * when that happens we no longer need to emit Remote1 and should encourage
 * users to use Remote exclusively. It is possible to yank this
 * ReporterFactory out of the code-base entirely at that point.
 *
 * @author Eric Crahen <crahen@amazon.com>
 * @version 1.0
 */
public class OldBsfMetricReporterFactory implements ReporterFactory {

  private final ReporterFactory reporterFactory;

  public OldBsfMetricReporterFactory(ReporterFactory reporterFactory) {
    if(reporterFactory == null)
      throw new IllegalArgumentException();
    this.reporterFactory = reporterFactory;
  }

  public Reporter newReporter() {

    return new Reporter() {

      private final Reporter r = reporterFactory.newReporter();

      // Lazily initialized to minimize the overhead of taking a sample
      // for each Group when we don't emit that sort of metric
      private volatile HashMap<Group, Selector> selectors;
      private int count = 0;

      public void beginReport() {
        synchronized(this) {
          count++;
        }
        r.beginReport();
      }

      public void addProperty(Group group, String name, CharSequence value) {
        r.addProperty(group, name, value);
      }

      public void addDate(Group group, String name, double value) {
        r.addDate(group, name, value);
      }

      public void addCount(Group group, String name, double value, Unit<?> unit, int repeat) {
        r.addCount(group, name, value, unit, repeat);
      }

      public void addLevel(Group group, String name, double value, Unit<?> unit, int repeat) {

        // Record a single datapoint as Remote1 before aggregation
        if("ServiceMetrics".equals(group.getName()) && "Remote".equals(name)) {

          synchronized(this) {
            // Create the state needed to track the samples on demand
            if(selectors == null)
              selectors = new HashMap<Group, Selector>();
            // Create a Selector for choosing a sample
            Selector selector = selectors.get(group);
            if(selector == null) {
              selector = new Selector(group);
              selectors.put(group, selector);
            }
            // We expect the repeat value to be one, and it usually is.
            // The only time it's not is when whoever is using the metrics
            // api isn't reporting data that way, in which case we do the
            // closest thing possible to take the sample.
            selector.addSample(value/repeat, unit);
          }

        }
        r.addLevel(group, name, value, unit, repeat);

      }

      public void addTime(Group group, String name, double value, Unit<?> unit, int repeat) {
        r.addTime(group, name, value, unit, repeat);
      }

      public void endReport() {
        synchronized(this) {
          // Before we close the reporter, publish any of the sampled metrics
          if(--count == 0) {
            if(selectors != null)
              for(Selector selector : selectors.values())
                r.addTime(selector.getGroup(), "Remote1", selector.getValue(), selector.getUnit(), 1);
          }
        }
        r.endReport();
      }

    };

  }

  // A Selector is responsible for making a random selection
  // when presented with a series of samples, the number of
  // which is not known apriori
  private static class Selector {

    private final static Random r = new Random();
    private final double point = r.nextDouble();
    private final Group group;
    private double count = 0;
    private double value = 0;
    private Unit<?> unit = Unit.ONE;

    public Selector(Group group) {
      this.group = group;
    }

    public void addSample(double value, Unit<?> unit) {
      if(count++ == 0) {
        this.value = value;
        this.unit = unit;
      } else if(1/count < point) {
        this.value = value;
        this.unit = unit;
      }
    }

    public Unit<?> getUnit() {
      return unit;
    }

    public double getValue() {
      return value;
    }

    public Group getGroup() {
      return group;
    }

  }

}
// Last update by RcsDollarAuthorDollar on RcsDollarDateDollar
