package com.amazon.coral.metrics.helper;

import java.io.Writer;
import java.io.IOException;
import java.util.TimeZone;
import java.util.List;

import com.amazon.coral.metrics.reporter.NullReporterFactory;
import com.amazon.coral.metrics.reporter.QuerylogReporterFactory;
import com.amazon.coral.metrics.reporter.SamplingReporterFactory;
import com.amazon.coral.metrics.reporter.ReporterFactory;
import com.amazon.coral.metrics.reporter.Reporter;
import com.amazon.coral.metrics.reporter.RecordWriter;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.coral.metrics.DefaultMetricsFactory;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.RollingFileWriter;
import com.amazon.coral.metrics.CompressedRollingFileWriter;
import com.amazon.coral.metrics.jmx.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@code QuerylogHelper} provides a simple {@code MetricsFactory} that
 * is simpler to configure with IoC toolkits that only support setter
 * injection. This {@code MetricsFactory} is specifically designed to make
 * it quite easy to configure a Querylog {@code Reporter} back end.
 * <p/>
 * <h2>Use with AppConfig</h2>
 *
 * This sample configuration demonstrates one way in which the {@code
 * AppConfig} class (which we recommend you move away from) can be instructed
 * to create a {@code QuerylogHelper}.
 *
 * <pre>
 * # Configure the new Metrics API
 * *.*.QuerylogHelper = {
 *     valueClass = com.amazon.coral.metrics.helper.QuerylogHelper;
 *     # Select a file name (required)
 *     filename = $ROOT/var/output/logs/service_log;
 *     # Select a time zone (default:UTC), such as "America/Los_Angeles". Put "local" to use system timezone.
 *     # timeZone = UTC;
 *     # Toggle enabled/disabled (default: enabled)
 *     # enabled = true;
 *     # Enable buffering (default: disabled)
 *     # recordsToBuffer = 50;
 *     # Enable dropping under contention
 *     # optimisticSampling = true;
 *     # minuteRotation = false;
 *     # writeToGzip = false;
 * };
 *
 * # Disable the ProfilerScope
 * *.*.Profiler.enableTracking = false;
 * *.*.Profiler.enableLogging = false;
 *
 * # Any log4j configuration with a key that has the
 * # prefix log4j.appender.performanceFile is no longer
 * # neccessary as this is used only by the Profiler/ProfilerScope
 * # code you've just disabled.
 * </pre>
 * <p/>
 *
 * <h2>Use with JST</h2>
 *
 * One way to surface this object within your Activity would be
 * to use the custom ActivityFactory feature. Either a custom ActivtyFactory
 * you write can be used; or an IoCActivityFactory can be used. Read about
 * this <a href="https://dse.amazon.com/?CodigoActivityFactoryProject">here</a> for more detail.
 *
 * Another option is to inject this into the ServiceContext for your
 * application. You can read more about ServiceContext configuration
 * <a href="https://dse.amazon.com/?JST:Config:Common_Tasks#CODIGO_ServiceContext">here</a>.
 *
 * @author Matt Wren &lt;wren@amazon.com&gt;
 */
public class QuerylogHelper implements MetricsFactory, ReporterFactory {

  private final static int DEFAULT_POLLING_INTERVAL = 60 * 5;

  private final static Sensor[] DEFAULT_JDK6_SENSORS = {
    new DiskUsageSensor(),
    new FileDescriptorSensor(),
    new CpuSensor(),
    new NetworkSensor(),
    new ThreadSensor(),
    new MemorySensor(),
    new UptimeSensor(),
    new GarbageCollectorSensor()
  };

  private final static Sensor[] DEFAULT_JDK5_SENSORS = {
    new FileDescriptorSensor(),
    new CpuSensor(),
    new NetworkSensor(),
    new ThreadSensor(),
    new MemorySensor(),
    new UptimeSensor(),
    new GarbageCollectorSensor()
  };

  private static boolean isJDK6() {
    try {
      Class.forName("java.util.concurrent.ConcurrentSkipListMap");
    } catch(Throwable t) {
      return false;
    }
    return true;
  }

  private final Log log = LogFactory.getLog(QuerylogHelper.class);

  private volatile MetricsFactory metricsFactory = null;
  private volatile ReporterFactory reporterFactory = null;
  private volatile JmxHelper jmxHelper = null;
  private String filename = null;
  private String timeZone = "UTC";
  private boolean enabled = true;
  private double samplingRate = -1.0;
  private int recordsToBuffer = 0;
  private boolean optimisticSampling = false;
  private boolean offloadEnabled = false;
  private int offloadTaskQueueSize = -1;
  private int offloadTaskQueueWorkerThreads = -1;
  private boolean minuteRotation = false;
  private boolean writeToGzip = false;

  private Sensor[] sensors = isJDK6() ? DEFAULT_JDK6_SENSORS : DEFAULT_JDK5_SENSORS;
  private int pollInterval = DEFAULT_POLLING_INTERVAL;

  // no default marketplace specified, field will be omitted by default
  private String marketplace = null;
  // no default program specified, field will be omitted by default
  private String program = null;

  /**
   */
  public void setRecordsToBuffer(int recordsToBuffer) {
    this.recordsToBuffer = recordsToBuffer;
  }

  /**
   */
  public void setOptimisticSampling(boolean optimisticSampling) {
    this.optimisticSampling = optimisticSampling;
  }

  /**
   */
  public void setOffloadEnabled(boolean offloadEnabled) {
    this.offloadEnabled = offloadEnabled;
  }

  /**
   */
  public void setOffloadTaskQueueSize(int offloadTaskQueueSize) {
    this.offloadTaskQueueSize = offloadTaskQueueSize;
  }
  
  /**
   */
  public void setOffloadTaskQueueWorkerThreads(int offloadTaskQueueWorkerThreads) {
    this.offloadTaskQueueWorkerThreads = offloadTaskQueueWorkerThreads;
  }
  
  /**
   */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  /**
   */
  public void setFilename(String filename) {
    if(log.isInfoEnabled())
      log.info("Querylog @ " + filename);
    this.filename = filename;
  }

  /**
   */
  public void setTimeZone(String timeZone) {
    this.timeZone = timeZone;
  }

  /**
   */
  public void setMarketplace(String marketplace) {
    this.marketplace = marketplace;
  }

  /**
   */
  public void setProgram(String program) {
    this.program = program;
  }

  /**
   */
  public void setSamplingRate(double samplingRate) {
    this.samplingRate = samplingRate;
  }

  /**
   */
  public void setSensors(List<Sensor> sensors) {
    this.sensors = sensors.toArray(new Sensor[sensors.size()]);
  }

  /**
   */
  public void setPollInterval(int interval) {
    this.pollInterval = interval;
  }

  /**
   * Minute log rotation. False by default.
   * When true, service logs are rotated every minute
   * and include the minute in the filename.
   * When false, logs are rotated every hour.
   */
  public void setMinuteRotation(boolean minuteRotation){
    this.minuteRotation = minuteRotation;
  }

  /**
   * Write log files straight to gzip. False by default.
   * When true, log files are written directly to gzip format.
   */
  public void setWriteToGzip(boolean writeToGzip) {
    this.writeToGzip = writeToGzip;
  }

  /**
   */
  private TimeZone getTimeZone() {
    if("local".equals(this.timeZone.toLowerCase()))
      return TimeZone.getDefault();
    TimeZone tz = TimeZone.getTimeZone(this.timeZone);
    if(tz.getID().equals(this.timeZone))
      return tz;
    throw new QuerylogHelperConfigurationException("The requested timezone is not recognized.  Requested=" + this.timeZone);
  }

  /**
   */
  protected MetricsFactory getMetricsFactory() {

    // Leverage the volatile guarantees to ensure any previously
    // set MetricsFactory is visible.
    if(this.metricsFactory != null)
      return this.metricsFactory;

    synchronized(this) {
      if(this.metricsFactory == null)
        this.metricsFactory = createFactory();
    }

    return this.metricsFactory;
  }

  /**
   */
  private MetricsFactory createFactory() {

    MetricsFactory createdFactory;

    if(null == filename || !enabled) {

      if(null == filename) {
        if(log.isInfoEnabled())
          log.info("Metrics disabled, filename was not set");
      }
      if(!enabled) {
        if(log.isInfoEnabled())
          log.info("Metrics disabled, explicit disable flag was set");
      }

      createdFactory = new NullMetricsFactory();

    } else {
      createdFactory = new DefaultMetricsFactory(this);
    }

    return createdFactory;

  }

  /**
   */
  protected void startJmxHelper() {
    if(sensors != null && sensors.length > 0)
      if(jmxHelper == null)
        synchronized(this) {
          if(jmxHelper == null)
            jmxHelper = new JmxHelper(this, pollInterval, sensors);
        }
  }

  /**
   */
  public Metrics newMetrics() {
    Metrics metrics =  prepopulateMetrics(getMetricsFactory().newMetrics());

    return new DelegateMetrics(metrics) {
      @Override
      public MetricsFactory getMetricsFactory() {
        return QuerylogHelper.this;
      }
    };
  }

  /**
   * Adds default entries to the Metrics object if they are available
   */
  private Metrics prepopulateMetrics(Metrics metrics) {

    if(marketplace != null)
      metrics.addProperty("Marketplace", marketplace);

    if(program != null)
      metrics.addProperty("Program", program);

    return metrics;

  }


  public Reporter newReporter() {
    return getReporterFactory().newReporter();
  }

  private ReporterFactory getReporterFactory() {
    if(reporterFactory != null)
      return reporterFactory;

    synchronized(this) {
      if(reporterFactory == null)
        reporterFactory = createReporterFactory();
    }

    return reporterFactory;
  }

  private ReporterFactory createReporterFactory() {
    if(filename == null)
      return new NullReporterFactory();
    else {
      QuerylogReporterFactory querylogReporterFactory;

      try {
        Writer writer;
        if (writeToGzip)
          writer = new CompressedRollingFileWriter(filename, getTimeZone(), minuteRotation);
        else
          writer = new RollingFileWriter(filename, getTimeZone(), minuteRotation);

        if(recordsToBuffer > 0)
          writer = new RecordWriter(writer, recordsToBuffer);

        querylogReporterFactory = 
                new QuerylogReporterFactory(writer, getTimeZone(), optimisticSampling, offloadEnabled, offloadTaskQueueSize, offloadTaskQueueWorkerThreads);
        startJmxHelper();

      } catch(IOException e) {
        throw new QuerylogHelperConfigurationException(e);
      }

      if(samplingRate >= 0.0 && samplingRate <= 1.0)
        return new SamplingReporterFactory(samplingRate, querylogReporterFactory);
      else
        return querylogReporterFactory;
    }
  }


}
// Last update by RcsDollarAuthorDollar on RcsDollarDateDollar
